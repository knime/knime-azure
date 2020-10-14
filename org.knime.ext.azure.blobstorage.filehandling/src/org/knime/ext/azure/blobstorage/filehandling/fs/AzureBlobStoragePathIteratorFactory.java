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
 *   2020-07-15 (Alexander Bondaletov): created
 */
package org.knime.ext.azure.blobstorage.filehandling.fs;

import java.io.IOException;
import java.nio.file.DirectoryStream.Filter;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.Iterator;
import java.util.stream.Collectors;

import org.knime.ext.azure.blobstorage.filehandling.AzureUtils;
import org.knime.filehandling.core.connections.base.PagedPathIterator;
import org.knime.filehandling.core.connections.base.attributes.BaseFileAttributes;

import com.azure.storage.blob.models.BlobContainerItem;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.ListBlobContainersOptions;
import com.azure.storage.blob.models.ListBlobsOptions;

/**
 * Factory for creating an iterator to iterate through
 * {@link AzureBlobStoragePath}.
 *
 * @author Alexander Bondaletov
 */
public final class AzureBlobStoragePathIteratorFactory {

    private AzureBlobStoragePathIteratorFactory() {
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
     */
    public static Iterator<AzureBlobStoragePath> create(final AzureBlobStoragePath path,
            final Filter<? super Path> filter) throws IOException {
        if (path.isRoot()) {
            return new ContainerIterator(path, filter);
        } else {
            return new BlobIterator(path.toDirectoryPath(), filter);
        }
    }

    private static final class ContainerIterator extends PagedPathIterator<AzureBlobStoragePath> {

        private ContainerIterator(final AzureBlobStoragePath path, final Filter<? super Path> filter)
                throws IOException {
            super(path, filter);
            setFirstPage(loadNextPage());
        }

        @Override
        protected boolean hasNextPage() {
            return false;
        }

        @SuppressWarnings("resource")
        @Override
        protected Iterator<AzureBlobStoragePath> loadNextPage() throws IOException {
            final AzureBlobStorageFileSystem fs = m_path.getFileSystem();
            try {
                return fs.getClient() //
                        .listBlobContainers(new ListBlobContainersOptions(), null) //
                        .stream() //
                        .map(this::toPath) //
                        .collect(Collectors.toList()) //
                        .iterator();
            } catch (BlobStorageException ex) {
                throw AzureUtils.toIOE(ex, m_path.toString());
            }
        }

        @SuppressWarnings("resource")
        private AzureBlobStoragePath toPath(final BlobContainerItem item) {
            final AzureBlobStorageFileSystem fs = m_path.getFileSystem();
            final AzureBlobStoragePath path = fs.getPath(fs.getSeparator() + item.getName(), fs.getSeparator());

            final FileTime createdAt = FileTime.fromMillis(0);
            final FileTime modifiedAt = FileTime.from(item.getProperties().getLastModified().toInstant());
            final BaseFileAttributes attrs = new BaseFileAttributes(false, path, modifiedAt, modifiedAt, createdAt, 0,
                    false, false, null);
            fs.addToAttributeCache(path, attrs);

            return path;
        }
    }

    private static final class BlobIterator extends PagedPathIterator<AzureBlobStoragePath> {

        private BlobIterator(final AzureBlobStoragePath path, final Filter<? super Path> filter) throws IOException {
            super(path, filter);
            setFirstPage(loadNextPage());
        }

        @Override
        protected boolean hasNextPage() {
            return false;
        }

        @SuppressWarnings("resource")
        @Override
        protected Iterator<AzureBlobStoragePath> loadNextPage() throws IOException {

            final AzureBlobStorageFileSystem fs = m_path.getFileSystem();
            final ListBlobsOptions opts = new ListBlobsOptions().setPrefix(m_path.getBlobName());

            try {
                return fs.getClient() //
                        .getBlobContainerClient(m_path.getBucketName()) //
                        .listBlobsByHierarchy(fs.getSeparator(), opts, null) //
                        .stream() //
                        .filter(blob -> !blob.getName().equals(m_path.getBlobName()))
                        .map(this::toPath) //
                        .collect(Collectors.toList()) //
                        .iterator();
            } catch (BlobStorageException ex) {
                throw AzureUtils.toIOE(ex, m_path.toString());
            }
        }

        @SuppressWarnings("resource")
        private AzureBlobStoragePath toPath(final BlobItem item) {
            final AzureBlobStorageFileSystem fs = m_path.getFileSystem();
            // Blob item doesn't have all the necessary info to construct BaseFileAttributes
            // so we don't do any attributes caching here
            return new AzureBlobStoragePath(fs, m_path.getBucketName(), item.getName());
        }
    }
}
