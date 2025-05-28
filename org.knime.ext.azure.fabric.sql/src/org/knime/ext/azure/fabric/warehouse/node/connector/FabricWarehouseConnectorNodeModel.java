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
 */
package org.knime.ext.azure.fabric.warehouse.node.connector;

import static org.knime.datatype.mapping.DataTypeMappingDirection.EXTERNAL_TO_KNIME;
import static org.knime.datatype.mapping.DataTypeMappingDirection.KNIME_TO_EXTERNAL;

import java.io.IOException;
import java.io.Serializable;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.context.ports.PortsConfiguration;
import org.knime.core.node.message.Message;
import org.knime.core.node.message.MessageBuilder;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.core.node.workflow.ICredentials;
import org.knime.core.node.workflow.VariableType;
import org.knime.core.webui.node.impl.WebUINodeModel;
import org.knime.credentials.base.NoSuchCredentialException;
import org.knime.database.DBType;
import org.knime.database.VariableContext;
import org.knime.database.connection.DBConnectionController;
import org.knime.database.datatype.mapping.DBDestination;
import org.knime.database.datatype.mapping.DBSource;
import org.knime.database.datatype.mapping.DBTypeMappingRegistry;
import org.knime.database.datatype.mapping.DBTypeMappingService;
import org.knime.database.dialect.DBSQLDialectRegistry;
import org.knime.database.driver.DBDriverRegistry;
import org.knime.database.driver.DBDriverWrapper;
import org.knime.database.extension.mssql.MSSQLServer;
import org.knime.database.extension.mssql.node.connector.MSAuthDBConnectionController;
import org.knime.database.port.DBSessionPortObject;
import org.knime.database.session.DBSession;
import org.knime.database.session.DBSessionCache;
import org.knime.database.session.DBSessionID;
import org.knime.database.session.DBSessionInformation;
import org.knime.database.session.impl.DefaultDBSessionInformation;
import org.knime.ext.azure.fabric.port.FabricConnection;
import org.knime.ext.azure.fabric.port.FabricWorkspacePortObject;
import org.knime.ext.azure.fabric.port.FabricWorkspacePortObjectSpec;
import org.knime.ext.azure.fabric.rest.sql.WarehouseAPI;
import org.knime.ext.azure.fabric.warehouse.FabricWarehouse;
import org.knime.node.datatype.mapping.DataTypeMappingConfigurationData;


/**
 * The Databricks Workspace Connector node model.
 *
 * @author Tobias Koetter, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings("restriction")
final class FabricWarehouseConnectorNodeModel extends WebUINodeModel<FabricWarehouseSettings> {


    private class NodeModelVariableContext implements VariableContext {

        @Override
        public ICredentials getCredentials(final String id) {
            return getCredentialsProvider().get(id);
        }

        @Override
        public Collection<String> getCredentialsIds() {
            return getCredentialsProvider().listNames();
        }

        @Override
        @Deprecated
        public Map<String, FlowVariable> getInputFlowVariables() {
            return getAvailableInputFlowVariables();
        }

        @Override
        public Map<String, FlowVariable> getInputFlowVariables(final VariableType<?>[] types) {
            return getAvailableFlowVariables(types);
        }

    }

    private static final DBType SQLSERVER_TYPE = MSSQLServer.DB_TYPE;

    private static final DBType DB_TYPE = FabricWarehouse.DB_TYPE;

    private final VariableContext m_variableContext = new NodeModelVariableContext();

    private DBSessionInformation m_sessionInfo;

    /**
     * @param portsConfig The node configuration.
     */
    FabricWarehouseConnectorNodeModel(final PortsConfiguration portsConfig) {
        super(portsConfig.getInputPorts(), portsConfig.getOutputPorts(), FabricWarehouseSettings.class);
    }

    @Override
    protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs,
            final FabricWarehouseSettings settings) throws InvalidSettingsException {

        if (!(inSpecs[0] instanceof FabricWorkspacePortObjectSpec)) {
                throw new InvalidSettingsException(
                        "Incompatible input connection. Connect the Fabric Workspace Connector output port.");
        }
        final var credentialRef = ((FabricWorkspacePortObjectSpec) inSpecs[0]).getFabricConnection().getCredential();
        FabricWarehouseCredentialUtil.validateCredentialOnConfigure(credentialRef);
        return new PortObjectSpec[] { null };
    }

    @Override
    protected PortObject[] execute(final PortObject[] inObjects, final ExecutionContext exec,
            final FabricWarehouseSettings settings) throws Exception {

        final FabricWorkspacePortObjectSpec spec = ((FabricWorkspacePortObject) inObjects[0]).getSpec();
        final FabricConnection connection = spec.getFabricConnection();

        final DBDriverWrapper driver = getDriver();
        final DBConnectionController controller = createConnectionController(settings, connection);
        m_sessionInfo = createSessionInfo(driver, controller);
        final DBSession session = registerSession(exec);

        final DBTypeMappingService<? extends DBSource, ? extends DBDestination> mappingService = DBTypeMappingRegistry
                .getInstance().getDBTypeMappingService(session.getDBType());

        return new PortObject[] { new DBSessionPortObject(session.getSessionInformation(), //
                DataTypeMappingConfigurationData //
                        .from(mappingService.createDefaultMappingConfiguration(KNIME_TO_EXTERNAL)), //
                DataTypeMappingConfigurationData //
                        .from(mappingService.createDefaultMappingConfiguration(EXTERNAL_TO_KNIME))) };
    }

    private static DBDriverWrapper getDriver() throws InvalidSettingsException {
        // Check if we or the user have registered a driver for Microsoft Fabric Data
        // Warehouse
        var latestDriver = DBDriverRegistry.getInstance().getLatestDriver(FabricWarehouse.DB_TYPE);
        if (latestDriver != null) {
            return latestDriver;
        }
        // Otherwise check if we have a driver for SQL Server
        latestDriver = DBDriverRegistry.getInstance().getLatestDriver(SQLSERVER_TYPE);
        if (latestDriver != null) {
            return latestDriver;
        }
        final MessageBuilder builder = Message.builder();
        throw builder.withSummary("No compatible driver found")
                .addResolutions("Install the KNIME Microsoft JDBC Driver For SQL Server extension",
                        "Register your own driver")
                .build().map(Message::toInvalidSettingsException).get();

    }

    private static DBConnectionController createConnectionController(final FabricWarehouseSettings settings,
            final FabricConnection connection)
            throws InvalidSettingsException, NoSuchCredentialException, IOException {
        final String warehouseId = settings.m_warehouseId;
        final WarehouseAPI api = connection.getAPI(WarehouseAPI.class);
        var warehouse = api.getWarehouse(connection.getWorkspaceId(), warehouseId);
        final var jdbcUrl = String.format("jdbc:sqlserver://%s:1433;database=%s", warehouse.properties.connectionString,
                warehouse.displayName);

        final var tokenAccessor = FabricWarehouseCredentialUtil.toAccessTokenAccessor(connection.getCredential());
        return new MSAuthDBConnectionController(tokenAccessor, jdbcUrl);
    }

    private static DBSessionInformation createSessionInfo(final DBDriverWrapper driver,
            final DBConnectionController connectionController) {

        final String dialectId = DBSQLDialectRegistry.getInstance().getDefaultFactoryFor(DB_TYPE).getId();
        final Map<String, ? extends Serializable> attributeValues = Collections.emptyMap();
        return new DefaultDBSessionInformation(DB_TYPE, dialectId, new DBSessionID(), driver.getDriverDefinition(),
                connectionController, attributeValues);
    }

    private DBSession registerSession(final ExecutionMonitor monitor) throws CanceledExecutionException, SQLException {
        Objects.requireNonNull(m_sessionInfo, "m_sessionInfo must not be null");

        final DBSession session = DBSessionCache.getInstance().getOrCreate(m_sessionInfo, m_variableContext, monitor);
        session.validate(monitor);
        return session;
    }

    @Override
    protected void onDispose() {
        destroySession();
    }

    @Override
    protected void reset() {
        destroySession();
    }

    private void destroySession() {
        if (m_sessionInfo != null) {
            m_sessionInfo.getConnectionController().cleanup();
            DBSessionCache.getInstance().destroy(m_sessionInfo.getID());
            m_sessionInfo = null;
        }
    }

}
