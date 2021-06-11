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
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;

import org.knime.ext.azure.AzureUtils;
import org.knime.ext.microsoft.authentication.port.MicrosoftCredential.Type;
import org.knime.filehandling.core.connections.base.BaseFileSystem;

import com.azure.core.http.HttpPipeline;
import com.azure.core.http.HttpPipelineBuilder;
import com.azure.core.http.policy.HttpPipelinePolicy;
import com.azure.core.http.policy.TimeoutPolicy;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobClientBuilder;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.BlobStorageException;

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

    private final AzureBlobStorageFSConnectionConfig m_config;

    private final boolean m_credentialsCanListContainers;

    /**
     * Creates a new instance.
     *
     * @param config
     *            Connection configuration
     * @param client
     *
     * @param cacheTTL
     *            The time to live for cached elements in milliseconds.
     * @throws IOException
     */
    public AzureBlobStorageFileSystem(final AzureBlobStorageFSConnectionConfig config, final BlobServiceClient client,
            final long cacheTTL)
            throws IOException {
        super(new AzureBlobStorageFileSystemProvider(), //
                cacheTTL, //
                config.getWorkingDirectory(), //
                AzureBlobStorageFSConnectionConfig.createFSLocationSpec(client.getAccountName()));

        m_config = config;
        m_client = client;
        m_credentialsCanListContainers = ensureSuccessfulAuthentication();
    }

    /**
     * Tests whether Blob storage can be accessed with the given credentials.
     *
     * @return true the user could list containers, false if the service did not
     *         reject the credentials but they don't have permission to list
     *         containers.
     * @throws IOException
     *             If authentication failed completely.
     */
    private boolean ensureSuccessfulAuthentication() throws IOException {
        try {
            // initialize lazy iterator by calling haxNext to make list containers request
            m_client.listBlobContainers().iterator().hasNext();// NOSONAR
            return true;
        } catch (BlobStorageException ex) {
            // rethrows the given exception as IOE, if error is non-recoverable
            AzureUtils.handleAuthException(ex);
            return false;
        }
    }

    /**
     * @return true if the provided credentials have permission to list containers,
     *         false otherwise.
     */
    public boolean canCredentialsListContainers() {
        return m_credentialsCanListContainers;
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
    public String getAccountName() {
        return m_client.getAccountName();
    }

    /**
     * @return the type of credential used to authenticate against Azure Blob
     *         Storage.
     */
    public Type getCredentialType() {
        return m_config.getCredential().getType();
    }

    @Override
    protected void prepareClose() throws IOException {
        // nothing to close
    }

    @Override
    public AzureBlobStoragePath getPath(final String first, final String... more) {
        return new AzureBlobStoragePath(this, first, more);
    }

    @Override
    public String getSeparator() {
        return PATH_SEPARATOR;
    }

    @Override
    public Iterable<Path> getRootDirectories() {
        return Collections.singletonList(getPath(PATH_SEPARATOR));
    }

    /**
     * @return whether to normalize paths
     */
    public boolean normalizePaths() {
        return m_config.isNormalizePaths();
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

        final long timeout = m_config.getTimeout().toSeconds();
        long newTimeout = Math.max(timeout, timeout * fileSize / FILE_SIZE_TIMEOUT_FACTOR);

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
