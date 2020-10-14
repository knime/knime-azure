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
package org.knime.ext.azure.blobstorage.filehandling.node;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.time.Duration;
import java.util.Arrays;
import java.util.regex.Pattern;

import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.ext.azure.blobstorage.filehandling.AzureUtils;
import org.knime.ext.azure.blobstorage.filehandling.OAuthTokenCredential;
import org.knime.ext.azure.blobstorage.filehandling.fs.AzureBlobStorageFSConnection;
import org.knime.ext.azure.blobstorage.filehandling.fs.AzureBlobStorageFileSystem;
import org.knime.ext.microsoft.authentication.port.MicrosoftCredential;
import org.knime.ext.microsoft.authentication.port.MicrosoftCredential.Type;
import org.knime.ext.microsoft.authentication.port.MicrosoftCredentialPortObject;
import org.knime.ext.microsoft.authentication.port.MicrosoftCredentialPortObjectSpec;
import org.knime.ext.microsoft.authentication.port.azure.storage.AzureSasTokenCredential;
import org.knime.ext.microsoft.authentication.port.azure.storage.AzureSharedKeyCredential;
import org.knime.ext.microsoft.authentication.port.oauth2.OAuth2Credential;
import org.knime.filehandling.core.connections.FSConnectionRegistry;
import org.knime.filehandling.core.port.FileSystemPortObject;
import org.knime.filehandling.core.port.FileSystemPortObjectSpec;

import com.azure.core.http.policy.TimeoutPolicy;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.common.StorageSharedKeyCredential;

import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * Azure Blob Storage Connector node.
 *
 * @author Alexander Bondaletov
 */
public class AzureBlobStorageConnectorNodeModel extends NodeModel {

    private static final NodeLogger LOG = NodeLogger.getLogger(AzureBlobStorageConnectorNodeModel.class);

    private static final Pattern BLOB_STORAGE_SCOPE_PATTERN = Pattern.compile("https://[^.]+.blob.core.windows.net/.+");

    private static final String FILE_SYSTEM_NAME = "Azure Blob Storage";

    private String m_fsId;
    private AzureBlobStorageFSConnection m_fsConnection;

    private final AzureBlobStorageConnectorSettings m_settings = new AzureBlobStorageConnectorSettings();

    /**
     * Creates new instance.
     */
    protected AzureBlobStorageConnectorNodeModel() {
        super(new PortType[] { MicrosoftCredentialPortObject.TYPE }, new PortType[] { FileSystemPortObject.TYPE });
    }

    @Override
    protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        MicrosoftCredential credential = ((MicrosoftCredentialPortObjectSpec) inSpecs[0]).getMicrosoftCredential();
        if (credential == null) {
            throw new InvalidSettingsException("Not authenticated");
        }

        m_fsId = FSConnectionRegistry.getInstance().getKey();
        return new PortObjectSpec[] { createSpec(credential) };
    }

    private FileSystemPortObjectSpec createSpec(final MicrosoftCredential credential) {
        final String storageAccount = extractStorageAccount(credential);
        return new FileSystemPortObjectSpec(FILE_SYSTEM_NAME, m_fsId,
                AzureBlobStorageFileSystem.createFSLocationSpec(storageAccount));
    }

    @Override
    protected PortObject[] execute(final PortObject[] inObjects, final ExecutionContext exec) throws Exception {
        MicrosoftCredential credential = ((MicrosoftCredentialPortObject) inObjects[0]).getMicrosoftCredentials();
        BlobServiceClient client = createServiceClient(credential, m_settings);

        try {
            // initialize lazy iterator by calling haxNext to make list containers request
            client.listBlobContainers().iterator().hasNext();// NOSONAR
        } catch (BlobStorageException ex) {
            handleBlobStorageException(ex);
        }

        m_fsConnection = new AzureBlobStorageFSConnection(client, m_settings);
        FSConnectionRegistry.getInstance().register(m_fsId, m_fsConnection);

        return new PortObject[] { new FileSystemPortObject(createSpec(credential)) };
    }

    private static final Pattern[] INVALID_CREDENTIAL_MESSAGES = new Pattern[] {
            Pattern.compile(
                    "The MAC signature found in the HTTP request .* is not the same as any computed signature."), //
            Pattern.compile("Signature fields not well formed"), //
            Pattern.compile("Signature did not match."), //
            Pattern.compile(".* is mandatory. Cannot be empty") };

    private void handleBlobStorageException(final BlobStorageException ex) throws IOException {
        if (ex.getStatusCode() == HttpResponseStatus.FORBIDDEN.code()) {
            if (Arrays.stream(INVALID_CREDENTIAL_MESSAGES).anyMatch(p -> p.matcher(ex.getMessage()).find())) {
                throw new IOException(
                        "Authentication failure. Please check your credentials in the Microsoft Authentication node.",
                        ex);
            } else {
                // there are probably more situations where authentication fails, which are not
                // accounted for in INVALID_CREDENTIAL_MESSAGES. To facilitate debugging we are
                // logging the
                // actual exception
                LOG.debug("Failed to list containers in storage account: " + ex.getMessage(), ex);
                setWarningMessage(
                        "Authentication failed, or the account doesn't have enough permissions to list containers");
            }
        } else {
            throw AzureUtils.toIOE(ex, AzureBlobStorageFileSystem.PATH_SEPARATOR);
        }
    }

    static BlobServiceClient createServiceClient(final MicrosoftCredential credential,
            final AzureBlobStorageConnectorSettings settings) throws IOException {

        BlobServiceClientBuilder builder = new BlobServiceClientBuilder()
                .addPolicy(new TimeoutPolicy(Duration.ofSeconds(settings.getTimeout())));
        Type type = credential.getType();

        switch (type) {
        case AZURE_SHARED_KEY:
            AzureSharedKeyCredential c = (AzureSharedKeyCredential) credential;
            builder.endpoint(c.getEndpoint());
            builder.credential(new StorageSharedKeyCredential(c.getAccount(), c.getSecretKey()));
            break;
        case AZURE_SAS_TOKEN:
            builder.endpoint(((AzureSasTokenCredential) credential).getSasUrl());
            break;
        case OAUTH2_ACCESS_TOKEN:
            final OAuth2Credential oauth2Credential = (OAuth2Credential) credential;
            builder.endpoint(extractEndpoint(oauth2Credential));
            builder.credential(new OAuthTokenCredential(oauth2Credential.getAccessToken()));
            break;
        default:
            throw new UnsupportedOperationException("Unsupported credential type " + type);
        }

        return builder.buildClient();
    }

    private static String extractEndpoint(final OAuth2Credential credential) {
        final String scope = credential.getScopes().stream() //
                .filter(BLOB_STORAGE_SCOPE_PATTERN.asPredicate()) //
                .findFirst() //
                .orElseThrow(() -> new IllegalStateException(
                        "Credentials do not provide access to Azure Blob Storage. Please request the appropriate access in the Microsoft Authentication node."));

        try {
            return "https://" + new URL(scope).getHost();
        } catch (MalformedURLException ex) {
            throw new UnsupportedOperationException("Malformed scope: " + scope, ex);
        }
    }

    private static String extractStorageAccount(final MicrosoftCredential credential) {
        switch (credential.getType()) {
        case AZURE_SHARED_KEY:
            final AzureSharedKeyCredential sharedKeyCredential = (AzureSharedKeyCredential) credential;
            return sharedKeyCredential.getAccount();
        case AZURE_SAS_TOKEN:
            try {
                return extractStorageAccount(((AzureSasTokenCredential) credential).getSasUrl());
            } catch (IOException e) {
                throw new IllegalArgumentException(e);
            }
        case OAUTH2_ACCESS_TOKEN:
            final OAuth2Credential oauth2Credential = (OAuth2Credential) credential;
            return extractStorageAccount(extractEndpoint(oauth2Credential));
        default:
            throw new UnsupportedOperationException("Unsupported credential type " + credential.getType());
        }
    }

    private static String extractStorageAccount(final String urlString) {
        final URI url = URI.create(urlString);
        final String[] hostnameParts = url.getHost().split("\\.");
        return hostnameParts[0];
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadInternals(final File nodeInternDir, final ExecutionMonitor exec)
            throws IOException, CanceledExecutionException {
        setWarningMessage("Azure Blob Storage connection no longer available. Please re-execute the node.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveInternals(final File nodeInternDir, final ExecutionMonitor exec)
            throws IOException, CanceledExecutionException {
        // nothing to save
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        m_settings.saveSettingsTo(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_settings.validateSettings(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_settings.loadSettingsFrom(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void onDispose() {
        reset();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void reset() {
        if (m_fsConnection != null) {
            m_fsConnection.closeInBackground();
            m_fsConnection = null;
        }
        m_fsId = null;
    }

}
