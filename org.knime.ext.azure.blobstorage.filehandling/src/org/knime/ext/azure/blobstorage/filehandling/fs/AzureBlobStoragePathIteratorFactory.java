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
import java.nio.file.DirectoryIteratorException;
import java.nio.file.DirectoryStream.Filter;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.knime.filehandling.core.connections.base.attributes.BaseFileAttributes;

import com.azure.storage.blob.models.BlobContainerItem;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.ListBlobsOptions;

/**
 * Factory for creating an iterator to iterate through
 * {@link AzureBlobStoragePath}.
 *
 * @author Alexander Bondaletov
 */
public class AzureBlobStoragePathIteratorFactory {

    /**
     * Creates iterator instance.
     *
     * @param path
     *            path to iterate.
     * @param filter
     *            {@link Filter} instance.
     * @return The iterator.
     */
    public static Iterator<AzureBlobStoragePath> create(final AzureBlobStoragePath path,
            final Filter<? super Path> filter) {
        if (path.getNameCount() == 0) {
            return new ContainerIterator(path, filter);
        } else {
            return new BlobIterator(path.toDirectoryPath(), filter);
        }
    }

    private abstract static class AzureIterator<T> implements Iterator<AzureBlobStoragePath> {
        protected final AzureBlobStoragePath m_path;
        private final Filter<? super Path> m_filter;

        private final Iterator<T> m_iterator;
        private AzureBlobStoragePath m_next;

        protected AzureIterator(final AzureBlobStoragePath path, final Filter<? super Path> filter) {
            m_path = path;
            m_filter = filter;
            m_iterator = createIterator(path);
            m_next = findNext();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean hasNext() {
            return m_next != null;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public AzureBlobStoragePath next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            AzureBlobStoragePath toReturn = m_next;
            m_next = findNext();
            return toReturn;
        }

        private AzureBlobStoragePath findNext() {
            while(m_iterator.hasNext()) {
                AzureBlobStoragePath next = toPath(m_iterator.next());
                try {
                    if (m_filter.accept(next) && !m_path.equals(next)) {
                        return next;
                    }
                } catch (IOException ex) {
                    throw new DirectoryIteratorException(ex);
                }
            }
            return null;
        }

        protected abstract Iterator<T> createIterator(AzureBlobStoragePath path);

        protected abstract AzureBlobStoragePath toPath(T item);
    }

    private static class ContainerIterator extends AzureIterator<BlobContainerItem> {
        private ContainerIterator(final AzureBlobStoragePath path, final Filter<? super Path> filter) {
            super(path, filter);
        }

        @SuppressWarnings("resource")
        @Override
        protected Iterator<BlobContainerItem> createIterator(final AzureBlobStoragePath path) {
            return m_path.getFileSystem().getClient().listBlobContainers().iterator();
        }

        @SuppressWarnings("resource")
        @Override
        protected AzureBlobStoragePath toPath(final BlobContainerItem item) {
            AzureBlobStorageFileSystem fs = m_path.getFileSystem();
            AzureBlobStoragePath path = fs.getPath(fs.getSeparator() + item.getName(), fs.getSeparator());

            FileTime createdAt = FileTime.fromMillis(0);
            FileTime modifiedAt = FileTime.from(item.getProperties().getLastModified().toInstant());
            BaseFileAttributes attrs = new BaseFileAttributes(false, path, modifiedAt, modifiedAt, createdAt, 0, false,
                    false, null);
            fs.addToAttributeCache(path, attrs);

            return path;
        }
    }

    private static class BlobIterator extends AzureIterator<BlobItem>{
        private BlobIterator(final AzureBlobStoragePath path, final Filter<? super Path> filter) {
            super(path, filter);
        }

        @SuppressWarnings("resource")
        @Override
        protected Iterator<BlobItem> createIterator(final AzureBlobStoragePath path) {
            AzureBlobStorageFileSystem fs = path.getFileSystem();

            ListBlobsOptions opts = new ListBlobsOptions().setPrefix(path.getBlobName());

            return fs.getClient().getBlobContainerClient(path.getBucketName())
                    .listBlobsByHierarchy(fs.getSeparator(), opts, null).iterator();
        }

        @SuppressWarnings("resource")
        @Override
        protected AzureBlobStoragePath toPath(final BlobItem item) {
            AzureBlobStorageFileSystem fs = m_path.getFileSystem();
            // Blob item doesn't have all the necessary info to construct BaseFileAttributes
            // so we don't do any attributes caching here
            return new AzureBlobStoragePath(fs, m_path.getBucketName(), item.getName());
        }
    }
}
