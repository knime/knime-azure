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

package org.knime.ext.azure.adls.gen2.filehandling.node;

import java.io.IOException;
import java.util.function.Supplier;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.webui.node.dialog.defaultdialog.internal.file.FSConnectionProvider;
import org.knime.core.webui.node.dialog.defaultdialog.internal.file.FileSelectionWidget;
import org.knime.core.webui.node.dialog.defaultdialog.internal.file.SingleFileSelectionMode;
import org.knime.core.webui.node.dialog.defaultdialog.internal.file.WithCustomFileSystem;
import org.knime.credentials.base.Credential;
import org.knime.credentials.base.CredentialPortObject;
import org.knime.ext.azure.adls.gen2.filehandling.fs.AdlsFSConnection;
import org.knime.ext.azure.adls.gen2.filehandling.fs.AdlsFSConnectionConfig;
import org.knime.ext.azure.adls.gen2.filehandling.fs.AdlsFileSystem;
import org.knime.node.parameters.Advanced;
import org.knime.node.parameters.NodeParameters;
import org.knime.node.parameters.NodeParametersInput;
import org.knime.node.parameters.Widget;
import org.knime.node.parameters.migration.LoadDefaultsForAbsentFields;
import org.knime.node.parameters.persistence.Persist;
import org.knime.node.parameters.updates.ParameterReference;
import org.knime.node.parameters.updates.StateProvider;
import org.knime.node.parameters.updates.ValueReference;
import org.knime.node.parameters.widget.number.NumberInputWidget;
import org.knime.node.parameters.widget.number.NumberInputWidgetValidation.MinValidation.IsPositiveIntegerValidation;


/**
 * Node parameters for Azure Data Lake Storage Gen2 Connector.
 *
 * @author Halil Yerlikaya, KNIME GmbH, Berlin, Germany
 * @author AI Migration Pipeline v1.2
 */
@LoadDefaultsForAbsentFields
class AdlsConnectorNodeParameters implements NodeParameters {

    @Widget(title = "Working directory", //
            description = """
                    Specify the working directory of the resulting file system connection, using the Path
                    syntax explained in the node description. The working directory must be specified as an
                    absolute path. A working directory allows downstream nodes to access files/folders using
                    relative paths, i.e. paths that do not have a leading slash. The default working directory
                    is the root "/".""")
    @FileSelectionWidget(SingleFileSelectionMode.FOLDER)
    @WithCustomFileSystem(connectionProvider = FileSystemConnectionProvider.class)
    @Persist(configKey = AdlsConnectorSettings.KEY_WORKING_DIRECTORY)
    String m_workingDirectory = AdlsFileSystem.PATH_SEPARATOR;

    @Widget(title = "Service calls timeout", //
            description = "The time in seconds allowed between sending a request and receiving the response.")
    @Advanced
    @NumberInputWidget(minValidation = IsPositiveIntegerValidation.class)
    @ValueReference(TimeoutRef.class)
    @Persist(configKey = AdlsConnectorSettings.KEY_TIMEOUT)
    int m_timeoutSeconds = AdlsFSConnectionConfig.DEFAULT_TIMEOUT;

    static final class TimeoutRef implements ParameterReference<Integer> {
    }

    /**
     * Provides a {@link FSConnectionProvider} based on the ADLS Gen2 connection
     * settings. This enables the working directory field to have a file system
     * browser.
     */
    static final class FileSystemConnectionProvider implements StateProvider<FSConnectionProvider> {

        private static final String ERROR_MSG = """
                Credential not available. Please ensure the preceding authenticator node is connected and re-execute it.
                """;

        private Supplier<Integer> m_timeoutSupplier;

        @Override
        public void init(final StateProviderInitializer initializer) {
            initializer.computeAfterOpenDialog();
            m_timeoutSupplier = initializer.computeFromValueSupplier(TimeoutRef.class);
        }

        @Override
        public FSConnectionProvider computeState(final NodeParametersInput parametersInput) {
            return () -> { // NOSONAR: acceptable length
                final var credential = parametersInput.getInPortObject(0) //
                        .filter(CredentialPortObject.class::isInstance) //
                        .map(CredentialPortObject.class::cast) //
                        .flatMap(cpo -> cpo.getCredential(Credential.class)) //
                        .orElseThrow(() -> new InvalidSettingsException(ERROR_MSG));

                final var config = AdlsConnectorSettings.toFSConnectionConfig(credential, AdlsFileSystem.PATH_SEPARATOR,
                        java.time.Duration.ofSeconds(m_timeoutSupplier.get()));
                return toFSConnection(config);
            };
        }

        private static AdlsFSConnection toFSConnection(final AdlsFSConnectionConfig config) throws IOException {
            final var connection = new AdlsFSConnection(config);
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
    static void testConnection(final AdlsFSConnection connection) throws IOException {
        if (!connection.getFileSystem().canCredentialsListContainers()) {
            throw new IOException(
                    "Authentication failed, or the account doesn't have enough permissions to list containers");
        }
    }
}
