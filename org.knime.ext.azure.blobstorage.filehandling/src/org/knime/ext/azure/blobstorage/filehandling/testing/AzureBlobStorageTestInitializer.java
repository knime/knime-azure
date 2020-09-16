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
 *   2020-07-16 (Alexander Bondaletov): created
 */
package org.knime.ext.azure.blobstorage.filehandling.testing;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import org.knime.ext.azure.blobstorage.filehandling.fs.AzureBlobStorageFileSystem;
import org.knime.ext.azure.blobstorage.filehandling.fs.AzureBlobStoragePath;
import org.knime.filehandling.core.connections.FSConnection;
import org.knime.filehandling.core.testing.DefaultFSTestInitializer;

import com.azure.core.http.rest.PagedIterable;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.ListBlobsOptions;

/**
 * Azure Blob Storage test initializer
 *
 * @author Alexander Bondaletov
 */
public class AzureBlobStorageTestInitializer
        extends DefaultFSTestInitializer<AzureBlobStoragePath, AzureBlobStorageFileSystem> {

    private BlobServiceClient m_client;

    /**
     * @param fsConnection
     */
    protected AzureBlobStorageTestInitializer(final FSConnection fsConnection) {
        super(fsConnection);
        m_client = getFileSystem().getClient();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AzureBlobStoragePath createFileWithContent(final String content, final String... pathComponents) throws IOException {
        AzureBlobStoragePath path = makePath(pathComponents);
        BlobContainerClient contClient = m_client.getBlobContainerClient(path.getBucketName());

        for (int i = 1; i < path.getNameCount() - 1; i++) {
            final String dirKey = path.subpath(1, i + 1).toString();
            BlobClient blobClient = contClient.getBlobClient(dirKey);
            if (!Boolean.TRUE.equals(blobClient.exists())) {
                blobClient.upload(new ByteArrayInputStream(new byte[0]), 0);
            }
        }

        byte[] bytes = content.getBytes();
        contClient.getBlobClient(path.getBlobName()).upload(new ByteArrayInputStream(bytes), bytes.length);
        return path;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void beforeTestCaseInternal() throws IOException {
        final AzureBlobStoragePath scratchDir = getTestCaseScratchDir().toDirectoryPath();
        m_client.getBlobContainerClient(scratchDir.getBucketName()).getBlobClient(scratchDir.getBlobName())
                .upload(new ByteArrayInputStream(new byte[0]), 0);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void afterTestCaseInternal() throws IOException {
        final AzureBlobStoragePath scratchDir = getTestCaseScratchDir().toDirectoryPath();
        BlobContainerClient contClient = m_client.getBlobContainerClient(scratchDir.getBucketName());

        ListBlobsOptions opts = new ListBlobsOptions().setPrefix(scratchDir.getBlobName());
        PagedIterable<BlobItem> blobs = contClient.listBlobs(opts, null);

        for (BlobItem item : blobs) {
            contClient.getBlobClient(item.getName()).delete();
        }
    }

}
