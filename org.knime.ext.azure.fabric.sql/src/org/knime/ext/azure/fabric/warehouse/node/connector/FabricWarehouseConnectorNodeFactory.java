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

import java.io.IOException;
import java.util.Optional;

import org.apache.xmlbeans.XmlException;
import org.knime.core.node.ConfigurableNodeFactory;
import org.knime.core.node.NodeDescription;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeView;
import org.knime.core.node.context.NodeCreationConfiguration;
import org.knime.core.node.context.ports.PortsConfiguration;
import org.knime.core.webui.node.dialog.NodeDialog;
import org.knime.core.webui.node.dialog.NodeDialogFactory;
import org.knime.core.webui.node.dialog.NodeDialogManager;
import org.knime.core.webui.node.dialog.SettingsType;
import org.knime.core.webui.node.dialog.defaultdialog.DefaultNodeDialog;
import org.knime.core.webui.node.impl.WebUINodeConfiguration;
import org.knime.core.webui.node.impl.WebUINodeFactory;
import org.knime.database.port.DBSessionPortObject;
import org.knime.ext.azure.fabric.port.FabricWorkspacePortObject;
import org.xml.sax.SAXException;

/**
 * The Databricks Workspace Connector node factory.
 *
 * @author Tobias Koetter, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings("restriction")
public class FabricWarehouseConnectorNodeFactory extends ConfigurableNodeFactory<FabricWarehouseConnectorNodeModel>
    implements NodeDialogFactory {

    private static final String WORKSPACE_INPUT_NAME = "Microsoft Fabric Workspace Connection";

    private static final String DB_OUTPUT_NAME = "DB Connection";

    private static final String FULL_DESCRIPTION = """
            The Microsoft Fabric Data Warehouse Connector node allows to connect to a
            <a href='https://learn.microsoft.com/en-us/fabric/data-warehouse/data-warehousing'>Fabric Data Warehouse.
            </a> Once connected you can use the
            <a href='https://docs.knime.com/latest/db_extension_guide/index.html'>KNIME Database nodes</a>
            to work with your data in the Microsoft Fabric Data Warehouse.

            To upload large datasets to the Microsoft Fabric Data Warehouse, use the
            <a href='https://hub.knime.com/n/hp8L4I_m6lJfldj5'>DB Loader node.</a> It uses the
            <a href='https://learn.microsoft.com/en-us/sql/t-sql/statements/copy-into-transact-sql?view=fabric'>
            COPY INTO command</a> to load data via PArquet files into the Microsoft Fabric Data Warehouse.
            <a href='https://learn.microsoft.com/en-us/fabric/onelake/onelake-overview'>Fabric OneLake paths</a>
            are currently not supported, only BLOB and ADLS Gen2 storage accounts are supported. To use the
            DB Loader node, you need to connect it either to an
            <a href='https://hub.knime.com/n/-3zjQ7L-UWuUzzT6'>Azure Data Lake Storage Gen2 Connector node</a>
            or to an <a href='https://hub.knime.com/n/1iu8u7meRrzU-iPg'>Azure Blob Storage Connector node.</a>
            """;

    static final WebUINodeConfiguration CONFIG = WebUINodeConfiguration.builder()//
            .name("Microsoft Fabric Data Warehouse Connector")//
            .icon("./icon.png")//
            .shortDescription("Microsoft Fabric Data Warehouse Connector node.")//
        .fullDescription(FULL_DESCRIPTION) //
        .modelSettingsClass(FabricWarehouseSettings.class)//
            .addInputPort(WORKSPACE_INPUT_NAME, FabricWorkspacePortObject.TYPE,
                    "Microsoft Fabric Workspace connection")//
            .addOutputPort(DB_OUTPUT_NAME, DBSessionPortObject.TYPE,
                    "Microsoft Fabric Data Warehouse Connection")//
        .nodeType(NodeType.Source)//
            .sinceVersion(5, 5, 0)//
        .build();

    @Override
    protected NodeDescription createNodeDescription() throws SAXException, IOException, XmlException {
        return WebUINodeFactory.createNodeDescription(CONFIG);
    }

    @Override
    public NodeDialog createNodeDialog() {
        return new DefaultNodeDialog(SettingsType.MODEL, FabricWarehouseSettings.class);
    }

    @Override
    protected Optional<PortsConfigurationBuilder> createPortsConfigBuilder() {
        final PortsConfigurationBuilder b = new PortsConfigurationBuilder();
        b.addFixedInputPortGroup(WORKSPACE_INPUT_NAME, FabricWorkspacePortObject.TYPE);
        b.addFixedOutputPortGroup(DB_OUTPUT_NAME, DBSessionPortObject.TYPE);
        return Optional.of(b);
    }

    @Override
    protected FabricWarehouseConnectorNodeModel createNodeModel(final NodeCreationConfiguration creationConfig) {
        final PortsConfiguration portsConfig = creationConfig.getPortConfig().orElseThrow();
        return new FabricWarehouseConnectorNodeModel(portsConfig);
    }

    @Override
    protected NodeDialogPane createNodeDialogPane(final NodeCreationConfiguration creationConfig) {
        return NodeDialogManager.createLegacyFlowVariableNodeDialog(createNodeDialog());
    }

    @Override
    protected int getNrNodeViews() {
        return 0;
    }

    @Override
    public NodeView<FabricWarehouseConnectorNodeModel> createNodeView(final int viewIndex,
        final FabricWarehouseConnectorNodeModel nodeModel) {
        return null;
    }

    @Override
    protected boolean hasDialog() {
        return true;
    }
}
