/*
 * ------------------------------------------------------------------------
 *
 *  Copyright by KNIME AG, Zurich, Switzerland
 *  Website: http://www.knime.com; Email: contact@knime.com
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License, Version 3, as
 *  published by the Free Software Foundation.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses>.
 *
 *  Additional permission under GNU GPL version 3 section 7:
 *
 *  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
 *  Hence, KNIME and ECLIPSE are both independent programs and are not
 *  derived from each other. Should, however, the interpretation of the
 *  GNU GPL Version 3 ("License") under any applicable laws result in
 *  KNIME and ECLIPSE being a combined program, KNIME AG herewith grants
 *  you the additional permission to use and propagate KNIME together with
 *  ECLIPSE with only the license terms in place for ECLIPSE applying to
 *  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
 *  license terms of ECLIPSE themselves allow for the respective use and
 *  propagation of ECLIPSE together with KNIME.
 *
 *  Additional permission relating to nodes for KNIME that extend the Node
 *  Extension (and in particular that are based on subclasses of NodeModel,
 *  NodeDialog, and NodeView) and that only interoperate with KNIME through
 *  standard APIs ("Nodes"):
 *  Nodes are deemed to be separate and independent programs and to not be
 *  covered works.  Notwithstanding anything to the contrary in the
 *  License, the License does not apply to Nodes, you are not required to
 *  license Nodes under the License, and you are granted a license to
 *  prepare and propagate Nodes, in each case even if such Nodes are
 *  propagated with or for interoperation with KNIME.  The owner of a Node
 *  may freely choose the license terms applicable to such Node, including
 *  when such Node is propagated with or for interoperation with KNIME.
 * ---------------------------------------------------------------------
 *
 * History
 *   2020-07-14 (Alexander Bondaletov): created
 */
package org.knime.ext.azure.blobstorage.filehandling.fs;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;

import org.knime.ext.azure.blobstorage.filehandling.node.AzureBlobStorageConnectorSettings;
import org.knime.filehandling.core.connections.DefaultFSLocationSpec;
import org.knime.filehandling.core.connections.FSCategory;
import org.knime.filehandling.core.connections.FSLocationSpec;
import org.knime.filehandling.core.connections.base.BaseFileSystem;

import com.azure.core.http.HttpPipeline;
import com.azure.core.http.HttpPipelineBuilder;
import com.azure.core.http.policy.HttpPipelinePolicy;
import com.azure.core.http.policy.TimeoutPolicy;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobClientBuilder;
import com.azure.storage.blob.BlobServiceClient;

/**
 * Azure Blob Storage implementation of the {@link FileSystem} interface.
 *
 * @author Alexander Bondaletov
 */
public class AzureBlobStorageFileSystem extends BaseFileSystem<AzureBlobStoragePath> {

    /**
     * Character to use as path separator
     */
    public static final String PATH_SEPARATOR = "/";
    private static final long FILE_SIZE_TIMEOUT_FACTOR = 10 * 1024 * 1024L;// 10Mb

    private final BlobServiceClient m_client;
    private final boolean m_normalizePaths;
    private final int m_standardTimeout;

    /**
     * Creates a new instance.
     *
     * @param cacheTTL
     *            The time to live for cached elements in milliseconds.
     * @param client
     *            The {@link BlobServiceClient} instance.
     * @param settings
     *            The settings.
     */
    public AzureBlobStorageFileSystem(final BlobServiceClient client, final AzureBlobStorageConnectorSettings settings,
            final long cacheTTL) {
        super(new AzureBlobStorageFileSystemProvider(), //
                createURL(client.getAccountName()), //
                cacheTTL, settings.getWorkingDirectory(), //
                createFSLocationSpec(client.getAccountName()));

        m_client = client;
        m_normalizePaths = settings.shouldNormalizePaths();
        m_standardTimeout = settings.getTimeout();
    }

    private static URI createURL(final String accountName) {
        try {
            return new URI(AzureBlobStorageFileSystemProvider.FS_TYPE, accountName, null, null);
        } catch (URISyntaxException ex) {
            // never happens
            throw new IllegalArgumentException(accountName, ex);
        }
    }

    /**
     * @param accountName
     *            The storage account name.
     * @return the {@link FSLocationSpec} for a Azure Blob Storage file system.
     */
    public static DefaultFSLocationSpec createFSLocationSpec(final String accountName) {
        return new DefaultFSLocationSpec(FSCategory.CONNECTED, //
                String.format("%s:%s", AzureBlobStorageFileSystemProvider.FS_TYPE, accountName));
    }

    /**
     * @return the client
     */
    public BlobServiceClient getClient() {
        return m_client;
    }

    /**
     * @return account name used by client
     */
    String getAccountName() {
        return m_client.getAccountName();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void prepareClose() throws IOException {
        // nothing to close
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getSchemeString() {
        return provider().getScheme();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getHostString() {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AzureBlobStoragePath getPath(final String first, final String... more) {
        return new AzureBlobStoragePath(this, first, more);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getSeparator() {
        return PATH_SEPARATOR;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterable<Path> getRootDirectories() {
        return Collections.singletonList(getPath(PATH_SEPARATOR));
    }

    /**
     * @return whether to normalize paths
     */
    public boolean normalizePaths() {
        return m_normalizePaths;
    }

    /**
     * Returns {@link BlobClient} instance where {@link TimeoutPolicy} is replaced
     * with a new one with a larger timeout value. New timeout value is calculated
     * as standard timeout (from the node settings) multiplied by the value derived
     * from the file size.
     *
     * @param container
     *            The container name.
     * @param blob
     *            The blob name.
     * @param fileSize
     *            The file size in bytes.
     * @return The {@link BlobClient} instance.
     */
    public BlobClient getBlobClientwithIncreasedTimeout(final String container, final String blob,
            final long fileSize) {
        long newTimeout = Math.max(m_standardTimeout, m_standardTimeout * fileSize / FILE_SIZE_TIMEOUT_FACTOR);

        HttpPipeline pipeline = m_client.getHttpPipeline();
        HttpPipelinePolicy[] policies = new HttpPipelinePolicy[pipeline.getPolicyCount()];

        for (int i = 0; i < pipeline.getPolicyCount(); i++) {
            HttpPipelinePolicy policy = pipeline.getPolicy(i);

            if (policy instanceof TimeoutPolicy) {
                policy = new TimeoutPolicy(Duration.ofSeconds(newTimeout));
            }

            policies[i] = policy;
        }

        HttpPipeline newPipeline = new HttpPipelineBuilder().policies(policies).httpClient(pipeline.getHttpClient())
                .build();

        return new BlobClientBuilder() //
                .endpoint(m_client.getAccountUrl()) //
                .containerName(container) //
                .blobName(blob) //
                .pipeline(newPipeline) //
                .buildClient();
    }
}
