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
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.util.Collections;

import org.knime.filehandling.core.connections.DefaultFSLocationSpec;
import org.knime.filehandling.core.connections.FSCategory;
import org.knime.filehandling.core.connections.FSLocationSpec;
import org.knime.filehandling.core.connections.base.BaseFileSystem;

import com.azure.storage.file.datalake.DataLakeServiceClient;

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

    /**
     * @param client
     *            The {@link DataLakeServiceClient} instance.
     * @param cacheTTL
     *            The time to live for cached elements in milliseconds.
     * @param workingDirectory
     *            The working directory.
     */
    protected AdlsFileSystem(final DataLakeServiceClient client, final long cacheTTL, final String workingDirectory) {
        super(new AdlsFileSystemProvider(), //
                createURL(client.getAccountName()), //
                cacheTTL, workingDirectory, //
                createFSLocationSpec(client.getAccountName()));

        m_client = client;
    }

    private static URI createURL(final String accountName) {
        try {
            return new URI(AdlsFileSystemProvider.FS_TYPE, accountName, null, null);
        } catch (URISyntaxException ex) {
            throw new IllegalArgumentException(accountName, ex);
        }
    }

    /**
     * @param accountName
     *            The storage account name.
     * @return the {@link FSLocationSpec} for a ADLS file system.
     */
    public static DefaultFSLocationSpec createFSLocationSpec(final String accountName) {
        return new DefaultFSLocationSpec(FSCategory.CONNECTED, //
                String.format("%s:%s", AdlsFileSystemProvider.FS_TYPE, accountName));
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
