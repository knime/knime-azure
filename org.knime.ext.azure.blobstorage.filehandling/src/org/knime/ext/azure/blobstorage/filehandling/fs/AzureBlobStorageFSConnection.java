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
 *   2020-07-14 (Alexander Bondaletov): created
 */
package org.knime.ext.azure.blobstorage.filehandling.fs;

import java.io.IOException;

import org.knime.credentials.base.oauth.api.JWTCredential;
import org.knime.ext.azure.AzureUtils;
import org.knime.ext.azure.TokenCredentialFactory;
import org.knime.ext.microsoft.authentication.credential.AzureStorageSasUrlCredential;
import org.knime.ext.microsoft.authentication.credential.AzureStorageSharedKeyCredential;
import org.knime.filehandling.core.connections.FSConnection;
import org.knime.filehandling.core.connections.base.BaseFSConnection;

import com.azure.core.http.policy.TimeoutPolicy;
import com.azure.core.util.HttpClientOptions;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.common.StorageSharedKeyCredential;

/**
 * Azure Blob Storage implementation of the {@link FSConnection} interface.
 *
 * @author Alexander Bondaletov
 */
public class AzureBlobStorageFSConnection extends BaseFSConnection {

    private static final long CACHE_TTL = 6000;

    private final AzureBlobStorageFileSystem m_filesystem;

    /**
     * @param config
     *            Connection configuration
     * @throws IOException
     */
    public AzureBlobStorageFSConnection(final AzureBlobStorageFSConnectionConfig config) throws IOException {
        final BlobServiceClient client = createServiceClient(config);
        m_filesystem = new AzureBlobStorageFileSystem(config, client, CACHE_TTL);
    }

    private static BlobServiceClient createServiceClient(final AzureBlobStorageFSConnectionConfig config) {

        final var credential = config.getCredential();

        final BlobServiceClientBuilder builder = new BlobServiceClientBuilder() //
                .endpoint(AzureUtils.getEndpoint(credential)) //
                .addPolicy(new TimeoutPolicy(config.getTimeout()));

        if (credential instanceof AzureStorageSharedKeyCredential sharedKeyCred) {
            builder.credential(new StorageSharedKeyCredential(//
                    sharedKeyCred.getStorageAccountName(), //
                    sharedKeyCred.getSharedKey()));
        } else if (credential instanceof AzureStorageSasUrlCredential) {
            // Do nothing. SAS token is a part of the endpoint
        } else if (credential instanceof JWTCredential jwtCredential) {
            builder.credential(TokenCredentialFactory.create(jwtCredential));
        } else {
            throw new UnsupportedOperationException("Unsupported credential type " + credential.getType());
        }

        if (AzureUtils.isProxyActive()) {
            final var clientOptions = new HttpClientOptions();
            clientOptions.setProxyOptions(AzureUtils.loadSystemProxyOptions());
            builder.clientOptions(clientOptions);
        }
        return builder.buildClient();
    }

    @Override
    public AzureBlobStorageFileSystem getFileSystem() {
        return m_filesystem;
    }
}
