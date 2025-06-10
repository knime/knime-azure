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
 *   2025-05-03 (Bjoern Lohrmann): created
 */
package org.knime.ext.azure.onelake.filehandling.fs;

import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.knime.ext.azure.AzureUtils;
import org.knime.ext.azure.TokenCredentialFactory;
import org.knime.filehandling.core.connections.base.BaseFileSystem;
import org.knime.filehandling.core.connections.base.attributes.BaseFileAttributes;

import com.azure.core.util.HttpClientOptions;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClientBuilder;

/**
 * OneLake implementation of the {@link FileSystem} interface.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public class OneLakeFileSystem extends BaseFileSystem<OneLakePath> {

    private static final String ONE_LAKE_ENDPOINT = "https://onelake.dfs.fabric.microsoft.com";

    /**
     * Character to use as path separator
     */
    public static final String PATH_SEPARATOR = "/";

    private final String m_workspaceId;

    private final String m_workspaceName;

    private final DataLakeFileSystemClient m_client;

    /**
     * Caches file attributes for managed paths. The cache lifetime is the lifetime
     * of the file system instance.
     */
    private final Map<String, BaseFileAttributes> m_managedPathAttributes = new HashMap<>();


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
    public OneLakeFileSystem(final OneLakeFSConnectionConfig config, final long cacheTTL) throws IOException {

        super(new OneLakeFileSystemProvider(), //
                cacheTTL, //
                config.getWorkingDirectory(), //
                OneLakeFSConnectionConfig.createFSLocationSpec(config.getWorkspaceId()));

        m_workspaceId = config.getWorkspaceId();
        m_workspaceName = config.getWorkspaceName();
        m_client = createClient(config);
    }


    private static DataLakeFileSystemClient createClient(final OneLakeFSConnectionConfig config) {

        final var clientBuilder = new DataLakeFileSystemClientBuilder()//
                .endpoint(ONE_LAKE_ENDPOINT)//
                .clientOptions(createHttpClientOptions(config))//
                .credential(TokenCredentialFactory.create(config.getAccessTokenAccessor()))//
                .fileSystemName(config.getWorkspaceName());

        return clientBuilder.buildClient();
    }


    private static HttpClientOptions createHttpClientOptions(final OneLakeFSConnectionConfig config) {
        final var httpClientOptions = new HttpClientOptions();
        httpClientOptions.setConnectTimeout(config.getConnectionTimeout());
        httpClientOptions.setReadTimeout(config.getReadTimeout());
        if (AzureUtils.isProxyActive()) {
            httpClientOptions.setProxyOptions(AzureUtils.loadSystemProxyOptions());
        }
        return httpClientOptions;
    }


    /**
     * @return the underlying {@link DataLakeFileSystemClient}
     */
    public DataLakeFileSystemClient getClient() {
        return m_client;
    }

    String getWorkspaceId() {
        return m_workspaceId;
    }


    @Override
    protected void prepareClose() throws IOException {
        // nothing to close
    }

    @Override
    public OneLakePath getPath(final String first, final String... more) {
        return new OneLakePath(this, first, more);
    }

    @Override
    public String getSeparator() {
        return PATH_SEPARATOR;
    }

    @Override
    public Iterable<Path> getRootDirectories() {
        return Collections.singletonList(getPath(PATH_SEPARATOR));
    }

    String getFabricWorkspaceName() {
        return m_workspaceName;
    }

    synchronized void addManagedPathAttributes(final OneLakePath path, final BaseFileAttributes attributes) {
        final var pathString = ((OneLakePath) path.toAbsolutePath().normalize()).getFilePath();
        m_managedPathAttributes.put(pathString, attributes);
    }

    synchronized Optional<BaseFileAttributes> getManagedPathAttributes(final OneLakePath path) {
        final var pathString = ((OneLakePath) path.toAbsolutePath().normalize()).getFilePath();
        return Optional.ofNullable(m_managedPathAttributes.get(pathString));
    }
}
