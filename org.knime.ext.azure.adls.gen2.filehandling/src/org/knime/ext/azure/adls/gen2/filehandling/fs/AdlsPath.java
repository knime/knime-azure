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

import java.nio.file.Path;

import org.knime.filehandling.core.connections.FSFileSystem;
import org.knime.filehandling.core.connections.base.UnixStylePath;

import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;

/**
 * {@link Path} implementation for the {@link AdlsFileSystem}.
 *
 * @author Alexander Bondaletov
 */
public class AdlsPath extends UnixStylePath {

    /**
     * Creates path from the given path string.
     *
     * @param fileSystem
     *            the file system.
     * @param first
     *            The first name component.
     * @param more
     *            More name components. the string representation of the path.
     */
    protected AdlsPath(final FSFileSystem<?> fileSystem, final String first, final String[] more) {
        super(fileSystem, first, more);
    }


    @Override
    public AdlsFileSystem getFileSystem() {
        return (AdlsFileSystem) super.getFileSystem();
    }

    /**
     * @return the file system name part of the path
     */
    public String getFileSystemName() {
        if (!isAbsolute()) {
            throw new IllegalStateException("File system name cannot be determined for relative paths.");
        }
        if (m_pathParts.isEmpty()) {
            return null;
        }
        return m_pathParts.get(0);
    }

    /**
     * @return the path of the file i.e. the path without the file system name.
     */
    public String getFilePath() {
        if (!isAbsolute()) {
            throw new IllegalStateException("File path cannot be determined for relative paths.");
        }
        if (m_pathParts.size() <= 1) {
            return null;
        } else {
            return subpath(1, getNameCount()).toString();
        }
    }

    /**
     * Returns the {@link DataLakeFileSystemClient} instance corresponding to the
     * path or <code>null</code> if the path doesn't contain file system name (i.e.
     * path is a virtual root).
     *
     * @return The file system client instance.
     */
    @SuppressWarnings("resource")
    public DataLakeFileSystemClient getFileSystemClient() {
        String filesystem = getFileSystemName();
        if (filesystem != null) {
            return getFileSystem().getClient().getFileSystemClient(filesystem);
        } else {
            return null;
        }
    }

    /**
     * Returns {@link DataLakeFileClient} instance corresponding to the path or
     * <code>null</code> if the path doesn't contain the 'file path' part (i.e. path
     * is a virtual root or points to a filesystem).
     *
     * @return The file client instance.
     */
    public DataLakeFileClient getFileClient() {
        String filePath = getFilePath();
        if (filePath != null) {
            return getFileSystemClient().getFileClient(filePath);
        } else {
            return null;
        }
    }
}
