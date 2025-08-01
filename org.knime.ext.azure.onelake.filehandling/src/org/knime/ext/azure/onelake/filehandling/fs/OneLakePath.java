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

import java.nio.file.Path;

import org.knime.filehandling.core.connections.base.UnixStylePath;

import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;

/**
 * {@link Path} implementation for the {@link OneLakeFileSystem}.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public class OneLakePath extends UnixStylePath {

    /**
     * Creates path from the given path string.
     *
     * @param fileSystem
     *            the OneLake file system.
     * @param first
     *            The first name component.
     * @param more
     *            More name components. the string representation of the path.
     */
    protected OneLakePath(final OneLakeFileSystem fileSystem, final String first, final String[] more) {
        super(fileSystem, first, more);
    }


    @Override
    public OneLakeFileSystem getFileSystem() {
        return (OneLakeFileSystem) super.getFileSystem();
    }

    /**
     * @return the ADLSGen2 API file system name (aka Fabric workspace) part of the
     *         path in the OneLake URL
     */
    @SuppressWarnings("resource")
    public String getFileSystemName() {
        return getFileSystem().getFabricWorkspaceName();
    }

    /**
     * @return the path of the file in the ADLSGen2 file system (aka Fabric
     *         workspace), i.e. without the file system name.
     */
    public String getFilePath() {
        if (!isAbsolute()) {
            throw new IllegalStateException("File path cannot be determined for relative paths.");
        }

        return toString();
    }

    /**
     * Shortcut to get the {@link DataLakeFileSystemClient} instance from the
     * underlying {@link OneLakeFileSystem}.
     *
     * @return The {@link DataLakeFileSystemClient} instance.
     */
    @SuppressWarnings("resource")
    public DataLakeFileSystemClient getFileSystemClient() {
        return getFileSystem().getClient();
    }

    /**
     * Returns {@link DataLakeFileClient} instance corresponding to the path or
     * <code>null</code> if the path doesn't contain the 'file path' part (i.e. path
     * is a virtual root or points to a filesystem).
     *
     * @return The file client instance.
     */
    DataLakeFileClient getFileClient() {
        final var filePath = getFilePath();
        if (filePath != null) {
            return getFileSystemClient().getFileClient(filePath);
        } else {
            return null;
        }
    }

    /**
     * Checks whether this path is a Fabric-managed folder inside a Fabric
     * workspace, which means that it cannot be deleted or renamed and that its
     * properties cannot be retrieved via the ADLSGen2 API. Examples are Lakehouses,
     * Warehouses etc, as well as the folders within those items.
     *
     * @return true if this path is a managed folder, false otherwise.
     */
    public boolean isManagedPath() {
        // managed folders are below the root
        return toAbsolutePath().normalize().getNameCount() <= 2;
    }
}
