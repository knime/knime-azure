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
 *   2020-12-17 (Alexander Bondaletov): created
 */
package org.knime.ext.azure.adls.gen2.filehandling.fs;

import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.util.Collections;

import org.knime.ext.azure.AzureUtils;
import org.knime.filehandling.core.connections.base.BaseFileSystem;

import com.azure.core.util.HttpClientOptions;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder;
import com.azure.storage.file.datalake.models.DataLakeStorageException;

/**
 * ADLS implementation of the {@link FileSystem} interface.
 *
 * @author Alexander Bondaletov
 */
public class AdlsFileSystem extends BaseFileSystem<AdlsPath> {
    /**
     * Character to use as path separator
     */
    public static final String PATH_SEPARATOR = "/";

    private final DataLakeServiceClient m_client;

    private final boolean m_credentialsCanListContainers;

    /**
     * Constructor.
     *
     * @param config
     *            Connection configuration
     * @param cacheTTL
     *            The time to live for cached elements in milliseconds.
     * @throws IOException
     *             if something goes wrong while validating the connection.
     */
    public AdlsFileSystem(final AdlsFSConnectionConfig config, final long cacheTTL) throws IOException {

        super(new AdlsFileSystemProvider(), //
                cacheTTL, //
                config.getWorkingDirectory(), //
                config.getFSLocationSpec());

        m_client = createClient(config);
        m_credentialsCanListContainers = ensureSuccessfulAuthentication();
    }

    private static DataLakeServiceClient createClient(final AdlsFSConnectionConfig config) {

        final var httpClientOptions = new HttpClientOptions();
        httpClientOptions.setConnectTimeout(config.getConnectTimeout());
        httpClientOptions.setReadTimeout(config.getReadTimeout());
        if (AzureUtils.isProxyActive()) {
            httpClientOptions.setProxyOptions(AzureUtils.loadSystemProxyOptions());
        }

        final var clientBuilder = new DataLakeServiceClientBuilder()
                .endpoint(config.getEndpoint())//
                .clientOptions(httpClientOptions);

        if (config.getAzureTokenCredential() != null) {
            clientBuilder.credential(config.getAzureTokenCredential());
        } else if (config.getStorageSharedKeyCredential() != null) {
            clientBuilder.credential(config.getStorageSharedKeyCredential());
        }

        return clientBuilder.buildClient();
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
            m_client.listFileSystems().iterator().hasNext();// NOSONAR
            return true;
        } catch (DataLakeStorageException ex) {
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
    public DataLakeServiceClient getClient() {
        return m_client;
    }

    @Override
    protected void prepareClose() throws IOException {
        // nothing to close
    }

    @Override
    public AdlsPath getPath(final String first, final String... more) {
        return new AdlsPath(this, first, more);
    }

    @Override
    public String getSeparator() {
        return PATH_SEPARATOR;
    }

    @Override
    public Iterable<Path> getRootDirectories() {
        return Collections.singletonList(getPath(PATH_SEPARATOR));
    }

}
