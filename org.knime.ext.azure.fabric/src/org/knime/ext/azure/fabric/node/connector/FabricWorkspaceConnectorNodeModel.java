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

import java.io.File;
import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.time.Duration;

import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.context.ports.PortsConfiguration;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.webui.node.impl.WebUINodeModel;
import org.knime.credentials.base.CredentialPortObject;
import org.knime.credentials.base.CredentialPortObjectSpec;
import org.knime.credentials.base.CredentialRef;
import org.knime.ext.azure.fabric.port.FabricConnection;
import org.knime.ext.azure.fabric.port.FabricWorkspacePortObject;
import org.knime.ext.azure.fabric.port.FabricWorkspacePortObjectSpec;
import org.knime.ext.azure.fabric.rest.FabricRESTClient;
import org.knime.ext.azure.fabric.rest.workspace.WorkspaceAPI;

import jakarta.ws.rs.ForbiddenException;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.ProcessingException;
import jakarta.ws.rs.WebApplicationException;

/**
 * The Databricks Workspace Connector node model.
 *
 * @author Tobias Koetter, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings("restriction")
final class FabricWorkspaceConnectorNodeModel extends WebUINodeModel<FabricWorkspaceSettings> {


    /**
     * @param portsConfig The node configuration.
     */
    protected FabricWorkspaceConnectorNodeModel(final PortsConfiguration portsConfig) {
        super(portsConfig.getInputPorts(), portsConfig.getOutputPorts(), FabricWorkspaceSettings.class);
    }

    @Override
    protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs,
            final FabricWorkspaceSettings settings)
        throws InvalidSettingsException {

        settings.validate(inSpecs);

        final var credSpec = (CredentialPortObjectSpec) inSpecs[0];
        final CredentialRef credRef;
        if (credSpec != null) {
            FabricCredentialUtil.validateCredentialOnConfigure(credSpec);
            credRef = credSpec.toRef();
        } else {
            credRef = new CredentialRef();
        }

        return new PortObjectSpec[] { //
                createFabricSpec(settings, credRef) };
    }

    private static FabricWorkspacePortObjectSpec createFabricSpec(final FabricWorkspaceSettings settings,
            final CredentialRef credRef) {

        return new FabricWorkspacePortObjectSpec(new FabricConnection(//
                credRef, //
                settings.m_workspaceId, //
                Duration.ofSeconds(settings.m_connectionTimeout), //
                Duration.ofSeconds(settings.m_readTimeout)));
    }


    @Override
    protected PortObject[] execute(final PortObject[] inObjects, final ExecutionContext exec,
        final FabricWorkspaceSettings settings) throws Exception {

        final var credRef = ((CredentialPortObject) inObjects[0]).getSpec().toRef();

        // as a side effect this will also validate the presence of a credential and
        // fetch a Fabric-scoped access token if necessary
        FabricCredentialUtil.toAccessTokenAccessor(credRef);

        final var spec = createFabricSpec(settings, credRef);
        testConnection(spec);
        return new PortObject[] { new FabricWorkspacePortObject(spec) };
    }

    private static void testConnection(final FabricWorkspacePortObjectSpec spec) throws IOException {
        try {
            final var client = FabricRESTClient.fromFabricConnection(//
                    WorkspaceAPI.class, spec.getFabricConnection());
            makeTestCall(spec.getFabricConnection().getWorkspaceId(), client);
        } catch (NotFoundException ex) {
            throw new IOException("Specified workspace does not exist!", ex);
        } catch (ForbiddenException ex) {
            throw new IOException("Access to workspace was denied. "//
                    + "Please check that the user has access to the Fabric workspace.", ex);
        } catch (WebApplicationException ex) {
            throw new IOException("Could not get Fabric workspaces: " + ex.getMessage(), ex);
        } catch (SocketTimeoutException ex) {
            throw new IOException("Connection timed out!", ex);
        } catch (UnknownHostException ex) {
            throw new IOException("No route to host! Please check your Internet connection.", ex);
        } catch (ConnectException ex) {
            throw new IOException("Could not connect: " + ex.getMessage(), ex);
        } catch (Throwable t) { // NOSONAR from unwrapping cause, should never be a Throwable
            throw new IOException("Error while testing connection!", t);
        }
    }

    private static void makeTestCall(final String workspaceId, //
            final WorkspaceAPI client) throws Throwable { // NOSONAR comes from cause
        try {
            client.getWorkspace(workspaceId);
        } catch (ProcessingException wrapper) { // NOSONAR: interested in content
            throw wrapper.getCause();
        }
    }

    @Override
    protected void loadInternals(final File nodeInternDir, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {
        setWarningMessage("Credential not available anymore. Please re-execute the preceding authenticator node.");
    }
}
