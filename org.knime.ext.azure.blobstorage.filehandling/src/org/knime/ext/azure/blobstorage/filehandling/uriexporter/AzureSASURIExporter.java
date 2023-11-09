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
 *   2021-04-14 (Ayaz Ali Qureshi, KNIME GmbH, Berlin, Germany): created
 */
package org.knime.ext.azure.blobstorage.filehandling.uriexporter;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.AccessDeniedException;
import java.time.Duration;
import java.time.OffsetDateTime;

import org.knime.cloud.core.filehandling.signedurl.SignedUrlConfig;
import org.knime.credentials.base.oauth.api.AccessTokenCredential;
import org.knime.credentials.base.oauth.api.JWTCredential;
import org.knime.ext.azure.AzureUtils;
import org.knime.ext.azure.blobstorage.filehandling.fs.AzureBlobStorageFileSystem;
import org.knime.ext.azure.blobstorage.filehandling.fs.AzureBlobStoragePath;
import org.knime.ext.microsoft.authentication.credential.AzureStorageSasUrlCredential;
import org.knime.ext.microsoft.authentication.credential.AzureStorageSharedKeyCredential;
import org.knime.filehandling.core.connections.FSPath;
import org.knime.filehandling.core.connections.uriexport.URIExporter;
import org.knime.filehandling.core.connections.uriexport.base.BaseURIExporter;

import com.azure.core.exception.HttpResponseException;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.models.UserDelegationKey;
import com.azure.storage.blob.sas.BlobSasPermission;
import com.azure.storage.blob.sas.BlobServiceSasSignatureValues;

/**
 * {@link URIExporter} implementation using generating signed https:// urls to
 * access data on Azure Blob Storage.
 *
 * @author Ayaz Ali Qureshi, KNIME GmbH, Berlin, Germany
 */

final class AzureSASURIExporter extends BaseURIExporter<SignedUrlConfig> {

    /**
     * @param config
     *            SignedUrlConfig object
     */
    protected AzureSASURIExporter(final SignedUrlConfig config) {
        super(config);
    }

    @Override
    public URI toUri(final FSPath path) throws URISyntaxException {
        final AzureBlobStoragePath azurePath = (AzureBlobStoragePath) path.toAbsolutePath().normalize();
        try {
            return getSasUrl(azurePath, getConfig().getValidityDuration());
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    /**
     * Generate a publicly accessible URL via Azure SAS mechanism with public
     * settings
     *
     * @param path
     *            The path to generate the URL for.
     * @param validityDuration
     *            How long the URL shall be valid.
     * @return A SAS URL
     * @throws IOException
     *             when something went wrong trying to generate the SAS URL
     */
    @SuppressWarnings("resource")
    public static URI getSasUrl(final AzureBlobStoragePath path, final Duration validityDuration) throws IOException {
        final AzureBlobStorageFileSystem fs = path.getFileSystem();

        if (fs.getCredentialType() == AzureStorageSasUrlCredential.TYPE) {
            throw new AccessDeniedException("Generating SAS URLs is not supported, when the Azure "
                    + "Blob Storage connection itself is authenticated by a SAS URL.");
        }

        try {
            final OffsetDateTime start = OffsetDateTime.now();
            final OffsetDateTime end = start.plus(validityDuration);

            final BlobClient blobClient = fs.getClient() //
                    .getBlobContainerClient(path.getBucketName()) //
                    .getBlobClient(path.getBlobName());
            final BlobSasPermission permission = new BlobSasPermission().setReadPermission(true);
            final BlobServiceSasSignatureValues signatureValues = new BlobServiceSasSignatureValues(end, permission) //
                    .setStartTime(start);

            final String generatedSASParams;
            if (fs.getCredentialType() == AzureStorageSharedKeyCredential.TYPE) {
                generatedSASParams = blobClient.generateSas(signatureValues);
            } else if (fs.getCredentialType() == JWTCredential.TYPE
                    || fs.getCredentialType() == AccessTokenCredential.TYPE) {
                final UserDelegationKey key = fs.getClient().getUserDelegationKey(start, end);
                generatedSASParams = blobClient.generateUserDelegationSas(signatureValues, key);
            } else {
                throw new IllegalStateException("Unsupported credential type: " + fs.getCredentialType());
            }

            return URI.create(String.format("%s?%s", blobClient.getBlobUrl(), generatedSASParams));
        } catch (HttpResponseException ex) {
            throw AzureUtils.toIOE(ex, path.toString());
        }
    }

}