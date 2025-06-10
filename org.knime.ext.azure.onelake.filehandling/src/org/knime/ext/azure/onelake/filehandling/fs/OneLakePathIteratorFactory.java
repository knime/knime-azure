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
import java.nio.file.DirectoryStream.Filter;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.Iterator;
import java.util.Optional;

import org.knime.ext.azure.AzureUtils;
import org.knime.filehandling.core.connections.base.BasePathIterator;
import org.knime.filehandling.core.connections.base.attributes.BaseFileAttributes;

import com.azure.storage.file.datalake.models.DataLakeStorageException;
import com.azure.storage.file.datalake.models.ListPathsOptions;
import com.azure.storage.file.datalake.models.PathItem;

/**
 * Factory for creating an iterator to iterate through {@link OneLakePath}.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
final class OneLakePathIteratorFactory {

    private OneLakePathIteratorFactory() {
    }

    /**
     * Creates iterator instance.
     *
     * @param path
     *            path to iterate.
     * @param filter
     *            {@link Filter} instance.
     * @return The iterator.
     * @throws IOException
     *             if something went wrong while making the initial listing request.
     */
    public static Iterator<OneLakePath> create(final OneLakePath path, final Filter<? super Path> filter)
            throws IOException {

        return new PathsIterator(path, filter);
    }

    private static final class PathsIterator extends BasePathIterator<OneLakePath> {

        private PathsIterator(final OneLakePath path, final Filter<? super Path> filter) throws IOException {
            super(path, filter);

            final var client = m_path.getFileSystemClient();
            final var opts = new ListPathsOptions().setPath(m_path.getFilePath());

            try {
                final var iterator = client.listPaths(opts, null)//
                        .stream()//
                        .map(this::toPath)//
                        .iterator();

                setFirstPage(iterator);
            } catch (DataLakeStorageException ex) {
                throw AzureUtils.toIOE(ex, path.toString());
            }
        }

        @SuppressWarnings("resource")
        private OneLakePath toPath(final PathItem item) {
            final var fs = m_path.getFileSystem();
            final var itemName = item.getName();
            final var path = fs.getPath(fs.getSeparator() + itemName);

            final var lastModifiedTime = Optional.ofNullable(item.getLastModified())//
                    .map(t -> FileTime.from(t.toInstant()))//
                    .orElse(FileTime.fromMillis(0));
            final var creationTime = Optional.ofNullable(item.getCreationTime())//
                    .map(t -> FileTime.from(t.toInstant()))//
                    .orElse(lastModifiedTime);

            final var attributes = new BaseFileAttributes(//
                    !item.isDirectory(), //
                    path, //
                    lastModifiedTime, //
                    lastModifiedTime, //
                    creationTime, //
                    item.getContentLength(), //
                    false, //
                    false, //
                    null);
            fs.addToAttributeCache(path, attributes);

            if (path.isManagedPath()) {
                fs.addManagedPathAttributes(path, attributes);
            }

            return path;
        }
    }
}
