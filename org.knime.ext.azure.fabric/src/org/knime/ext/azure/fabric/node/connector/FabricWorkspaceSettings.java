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
*   May 16, 2024 (Bjoern Lohrmann, KNIME GmbH): created
*/
package org.knime.ext.azure.fabric.node.connector;

import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.knime.core.data.sort.AlphanumericComparator;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.webui.node.dialog.defaultdialog.DefaultNodeSettings;
import org.knime.core.webui.node.dialog.defaultdialog.layout.Layout;
import org.knime.core.webui.node.dialog.defaultdialog.layout.Section;
import org.knime.core.webui.node.dialog.defaultdialog.widget.NumberInputWidget;
import org.knime.core.webui.node.dialog.defaultdialog.widget.Widget;
import org.knime.core.webui.node.dialog.defaultdialog.widget.choices.ChoicesProvider;
import org.knime.core.webui.node.dialog.defaultdialog.widget.choices.StringChoice;
import org.knime.core.webui.node.dialog.defaultdialog.widget.choices.StringChoicesProvider;
import org.knime.core.webui.node.dialog.defaultdialog.widget.handler.WidgetHandlerException;
import org.knime.core.webui.node.dialog.defaultdialog.widget.updates.Reference;
import org.knime.core.webui.node.dialog.defaultdialog.widget.updates.ValueReference;
import org.knime.core.webui.node.dialog.defaultdialog.widget.validation.NumberInputWidgetValidation.MinValidation.IsNonNegativeValidation;
import org.knime.ext.azure.fabric.rest.FabricRESTClient;
import org.knime.ext.azure.fabric.rest.workspace.Workspace;
import org.knime.ext.azure.fabric.rest.workspace.WorkspaceAPI;
import org.knime.ext.azure.fabric.rest.workspace.WorkspaceUtil;

/**
 * Node settings for the Microsoft Fabric Workspace Connector node.
 *
 * @author Tobias Koetter, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings("restriction")
public class FabricWorkspaceSettings implements DefaultNodeSettings {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(FabricWorkspaceSettings.class);

    private static final Comparator<Workspace> COMPARATOR = Comparator.comparing(i -> i.displayName,
            AlphanumericComparator.NATURAL_ORDER);

    @Section(title = "Timeouts", advanced = true)
    interface ConnectionTimeoutsSection {
    }

    @ChoicesProvider(WorkspaceChoiceProvider.class)
    @Widget(title = "Microsoft Fabric workspace", //
            description = """
                    Select the \
                    <a href='https://learn.microsoft.com/fabric/fundamentals/workspaces'>Microsoft Fabric
                    workspace</a> to work with.
                    """)
    String m_workspaceId = "";

    @Widget(title = "Connection timeout (seconds)",
        description = """
                Timeout in seconds to establish a connection, or 0 for an infinite timeout.
                Used by this and downstream nodes connecting to Microsoft Fabric.
                    """, //
        advanced = true)
    @NumberInputWidget(minValidation = IsNonNegativeValidation.class)
    @Layout(ConnectionTimeoutsSection.class)
    @ValueReference(ConnectionTimeoutRef.class)
    int m_connectionTimeout = 30;

    @Widget(title = "Read timeout (seconds)",
            description = """
                    Timeout in seconds to read data from an established connection
                    or 0 for an infinite timeout. Used by this and downstream nodes connecting
                    to Microsoft Fabric.
                    """, //
        advanced = true)
    @NumberInputWidget(minValidation = IsNonNegativeValidation.class)
    @Layout(ConnectionTimeoutsSection.class)
    @ValueReference(ReadTimeoutRef.class)
    int m_readTimeout = 30;

    Duration getReadTimeout() {
        return Duration.ofSeconds(m_readTimeout);
    }

    Duration getConnectionTimeout() {
        return Duration.ofSeconds(m_readTimeout);
    }

    /**
     * @param inSpecs
     *            input {@link PortObjectSpec}
     */
    void validate(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        validateAdvancedSettings();
        validateWorkspaceId();
    }

    private void validateWorkspaceId() throws InvalidSettingsException {
        if (StringUtils.isBlank(m_workspaceId)) {
            throw new InvalidSettingsException("Select the Microsoft Fabric workspace to connect to.");
        }
    }

    private void validateAdvancedSettings() throws InvalidSettingsException {

        if (m_connectionTimeout < 0) {
            throw new InvalidSettingsException("Connection timeout must be a positive number.");
        }

        if (m_readTimeout < 0) {
            throw new InvalidSettingsException("Read timeout must be a positive number.");
        }
    }

    private static final class WorkspaceChoiceProvider implements StringChoicesProvider {

        private Supplier<Integer> m_connectionTimeout;
        private Supplier<Integer> m_readTimeout;

        @Override
        public void init(final StateProviderInitializer initializer) {
            initializer.computeAfterOpenDialog();
            m_connectionTimeout = initializer.computeFromValueSupplier(ConnectionTimeoutRef.class);
            m_readTimeout = initializer.computeFromValueSupplier(ReadTimeoutRef.class);
        }

        @Override
        public List<StringChoice> computeState(final DefaultNodeSettingsContext context) {
            try {

                final var client = FabricRESTClient.fromCredentialPort(//
                        WorkspaceAPI.class, //
                        context.getPortObjectSpecs(), //
                        Duration.ofSeconds(m_readTimeout.get()), //
                        Duration.ofSeconds(m_connectionTimeout.get()));

                final List<Workspace> workspaces = WorkspaceUtil.getAllWorkspaces(client);
                return workspaces.stream() //
                        .sorted(COMPARATOR) //
                        .map(i -> new StringChoice(i.id, i.displayName)) //
                        .collect(Collectors.toList());
            } catch (final Exception e) { // NOSONAR catch all exceptions here
                LOGGER.info("Unable to fetch Microsoft Fabric workspace list.", e);
                throw new WidgetHandlerException(
                        "Unable to fetch Microsoft Fabric workspace list (Reason: %s)".formatted(e.getMessage()));
            }
        }

    }

    static class ConnectionTimeoutRef implements Reference<Integer> {
    }

    static class ReadTimeoutRef implements Reference<Integer> {
    }
}
