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
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;

import org.knime.ext.azure.blobstorage.filehandling.node.AzureBlobStorageConnectorSettings;
import org.knime.filehandling.core.connections.DefaultFSLocationSpec;
import org.knime.filehandling.core.connections.FSCategory;
import org.knime.filehandling.core.connections.FSLocationSpec;
import org.knime.filehandling.core.connections.base.BaseFileSystem;

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

    private final BlobServiceClient m_client;
    private final boolean m_normalizePaths;
    private final Duration m_timeout;
    private final Duration m_longTimeout;

    /**
     * @param uri
     *            the URI for the file system
     * @param cacheTTL
     *            The time to live for cached elements in milliseconds.
     * @param client
     *            The {@link BlobServiceClient} instance.
     * @param settings
     *            The settings.
     */
    public AzureBlobStorageFileSystem(final URI uri, final long cacheTTL, final BlobServiceClient client,
            final AzureBlobStorageConnectorSettings settings) {
        super(new AzureBlobStorageFileSystemProvider(), uri, cacheTTL, settings.getWorkingDirectory(),
                createFSLocationSpec());

        m_client = client;
        m_normalizePaths = settings.getNormalizePaths();
        m_timeout = Duration.ofSeconds(settings.getTimeout());
        m_longTimeout = Duration.ofSeconds(settings.getLongTimeout());
    }

    /**
     * @return the {@link FSLocationSpec} for a Azure Blob Storage file system.
     */
    public static DefaultFSLocationSpec createFSLocationSpec() {
        return new DefaultFSLocationSpec(FSCategory.CONNECTED, AzureBlobStorageFileSystemProvider.FS_TYPE);
    }

    /**
     * @return the client
     */
    public BlobServiceClient getClient() {
        return m_client;
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
     * Returns the timeout for azure service calls
     *
     * @return the timeout
     */
    public Duration getTimeout() {
        return m_timeout;
    }

    /**
     * Returns the timeout for the longer service operations (upload/download/copy)
     *
     * @return the longTimeout
     */
    public Duration getLongTimeout() {
        return m_longTimeout;
    }
}
