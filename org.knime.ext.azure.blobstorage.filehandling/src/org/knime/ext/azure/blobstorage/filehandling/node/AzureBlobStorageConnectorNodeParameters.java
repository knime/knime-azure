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
 * ------------------------------------------------------------------------
 */

package org.knime.ext.azure.blobstorage.filehandling.node;

import java.io.IOException;
import java.util.function.Supplier;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.webui.node.dialog.defaultdialog.internal.file.FSConnectionProvider;
import org.knime.core.webui.node.dialog.defaultdialog.internal.file.FileSelectionWidget;
import org.knime.core.webui.node.dialog.defaultdialog.internal.file.SingleFileSelectionMode;
import org.knime.core.webui.node.dialog.defaultdialog.internal.file.WithCustomFileSystem;
import org.knime.credentials.base.Credential;
import org.knime.credentials.base.CredentialPortObject;
import org.knime.ext.azure.blobstorage.filehandling.fs.AzureBlobStorageFSConnection;
import org.knime.ext.azure.blobstorage.filehandling.fs.AzureBlobStorageFSConnectionConfig;
import org.knime.ext.azure.blobstorage.filehandling.fs.AzureBlobStorageFileSystem;
import org.knime.node.parameters.Advanced;
import org.knime.node.parameters.NodeParameters;
import org.knime.node.parameters.NodeParametersInput;
import org.knime.node.parameters.Widget;
import org.knime.node.parameters.layout.After;
import org.knime.node.parameters.layout.Layout;
import org.knime.node.parameters.layout.Section;
import org.knime.node.parameters.migration.LoadDefaultsForAbsentFields;
import org.knime.node.parameters.persistence.Persist;
import org.knime.node.parameters.updates.ParameterReference;
import org.knime.node.parameters.updates.StateProvider;
import org.knime.node.parameters.updates.ValueReference;
import org.knime.node.parameters.widget.number.NumberInputWidget;
import org.knime.node.parameters.widget.number.NumberInputWidgetValidation.MinValidation.IsPositiveIntegerValidation;

/**
 * Node parameters for Azure Blob Storage Connector.
 *
 * @author Halil Yerlikaya, KNIME GmbH, Berlin, Germany
 * @author AI Migration Pipeline v1.2
 */
@LoadDefaultsForAbsentFields
@SuppressWarnings("restriction")
final class AzureBlobStorageConnectorNodeParameters implements NodeParameters {

    @Section(title = "File System Settings")
    interface FileSystemSettingsSection {
    }

    @Section(title = "Connection Settings")
    @After(FileSystemSettingsSection.class)
    @Advanced
    interface ConnectionSettingsSection {
    }

    @Widget(title = "Working directory", //
            description = """
                    Specifies the <i>working directory</i> using the path syntax explained above. \
                    The working directory must be specified as an absolute path. \
                    A working directory allows downstream nodes to access files/folders using <i>relative</i> paths, \
                    i.e. paths that do not have a leading slash. \
                    If not specified, the default working directory is <tt>/</tt>.""")
    @FileSelectionWidget(SingleFileSelectionMode.FOLDER)
    @WithCustomFileSystem(connectionProvider = FileSystemConnectionProvider.class)
    @Layout(FileSystemSettingsSection.class)
    @Persist(configKey = AzureBlobStorageConnectorSettings.KEY_WORKING_DIRECTORY)
    String m_workingDirectory = AzureBlobStorageFileSystem.PATH_SEPARATOR;

    @Widget(title = "Normalize paths", //
            description = """
                    Determines if the path normalization should be applied. Path normalization eliminates redundant \
                    components of a path like, e.g. <tt>/a/../b/./c</tt> can be normalized to <tt>/b/c</tt>. \
                    When these redundant components like <tt>../</tt> or <tt>.</tt> are part of an existing object, \
                    then normalization must be deactivated in order to access them properly.""")
    @Layout(FileSystemSettingsSection.class)
    @ValueReference(NormalizePathsRef.class)
    @Persist(configKey = AzureBlobStorageConnectorSettings.KEY_NORMALIZE_PATHS)
    boolean m_normalizePaths = true;

    @Widget(title = "Service calls timeout (seconds)", //
            description = "The time allowed between sending a request and receiving the response.")
    @Layout(ConnectionSettingsSection.class)
    @NumberInputWidget(minValidation = IsPositiveIntegerValidation.class)
    @ValueReference(TimeoutRef.class)
    @Persist(configKey = AzureBlobStorageConnectorSettings.KEY_TIMEOUT)
    int m_timeout = AzureBlobStorageFSConnectionConfig.DEFAULT_TIMEOUT;

    static final class TimeoutRef implements ParameterReference<Integer> {
    }

    static final class NormalizePathsRef implements ParameterReference<Boolean> {
    }

    /**
     * Provides a {@link FSConnectionProvider} based on the Azure Blob Storage
     * connection settings. This enables the working directory field to have a file
     * system browser.
     */
    static final class FileSystemConnectionProvider implements StateProvider<FSConnectionProvider> {

        private static final String ERROR_MSG = """
                Credential not available. Please re-execute the preceding authenticator node \
                and make sure it is connected.
                """;

        private Supplier<Integer> m_timeoutSupplier;
        private Supplier<Boolean> m_normalizePathsSupplier;

        @Override
        public void init(final StateProviderInitializer initializer) {
            initializer.computeAfterOpenDialog();
            m_timeoutSupplier = initializer.computeFromValueSupplier(TimeoutRef.class);
            m_normalizePathsSupplier = initializer.computeFromValueSupplier(NormalizePathsRef.class);
        }

        @Override
        public FSConnectionProvider computeState(final NodeParametersInput parametersInput) {
            return () -> { // NOSONAR: Acceptable length of lambda expression
                final var credential = parametersInput.getInPortObject(0) //
                        .filter(CredentialPortObject.class::isInstance) //
                        .map(CredentialPortObject.class::cast) //
                        .flatMap(cpo -> cpo.getCredential(Credential.class)) //
                        .orElseThrow(() -> new InvalidSettingsException(ERROR_MSG));

                final var config = toFSConnectionConfig(AzureBlobStorageFileSystem.PATH_SEPARATOR, credential,
                        m_timeoutSupplier.get(), m_normalizePathsSupplier.get());
                return toFSConnection(config);
            };
        }

        private static AzureBlobStorageFSConnectionConfig toFSConnectionConfig(final String workingDirectory,
                final Credential credential, final int timeout, final boolean normalizePaths) {
            final var config = new AzureBlobStorageFSConnectionConfig(workingDirectory);
            config.setCredential(credential);
            config.setTimeout(java.time.Duration.ofSeconds(timeout));
            config.setNormalizePaths(normalizePaths);
            return config;
        }

        private static AzureBlobStorageFSConnection toFSConnection(final AzureBlobStorageFSConnectionConfig config)
                throws IOException {
            final var connection = new AzureBlobStorageFSConnection(config);
            testConnection(connection);
            return connection;
        }
    }

    /**
     * Tests the connection by attempting to list containers. This is used by the
     * file browser to validate the connection before showing the file system.
     *
     * @param connection
     *            the connection to test
     * @throws IOException
     *             if the connection test fails
     */
    static void testConnection(final AzureBlobStorageFSConnection connection) throws IOException {
        // Test the connection by checking if we can list containers
        if (!connection.getFileSystem().canCredentialsListContainers()) {
            throw new IOException(
                    "Authentication failed, or the account doesn't have enough permissions to list containers");
        }
    }
}
