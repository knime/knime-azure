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
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.knime.core.data.sort.AlphanumericComparator;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.webui.node.dialog.defaultdialog.widget.handler.WidgetHandlerException;
import org.knime.ext.azure.fabric.port.FabricConnection;
import org.knime.ext.azure.fabric.port.FabricWorkspacePortObjectSpec;
import org.knime.ext.azure.fabric.rest.sql.Warehouse;
import org.knime.ext.azure.fabric.rest.sql.WarehouseAPI;
import org.knime.ext.azure.fabric.rest.sql.Warehouses;
import org.knime.node.parameters.NodeParameters;
import org.knime.node.parameters.NodeParametersInput;
import org.knime.node.parameters.Widget;
import org.knime.node.parameters.widget.choices.ChoicesProvider;
import org.knime.node.parameters.widget.choices.StringChoice;
import org.knime.node.parameters.widget.choices.StringChoicesProvider;

/**
 * Node settings for the Microsoft Fabric Workspace Connector node.
 *
 * @author Tobias Koetter, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings("restriction")
public class FabricWarehouseSettings implements NodeParameters {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(FabricWarehouseSettings.class);

    private static final Comparator<Warehouse> COMPARATOR = Comparator.comparing(i -> i.displayName,
            AlphanumericComparator.NATURAL_ORDER);


    @ChoicesProvider(WarehouseChoiceProvider.class)
    @Widget(title = "Microsoft Fabric Data Warehouse", //
            description = "Select the Microsoft Fabric Data Warehouse to use.")
    String m_warehouseId = "";


    /**
     * @param inSpecs
     *            input {@link PortObjectSpec}
     */
    void validate(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {

        if (StringUtils.isBlank(m_warehouseId)) {
            throw new InvalidSettingsException("No Microsoft Fabric Data Warehouse selected");
        }
    }

    private static final class WarehouseChoiceProvider implements StringChoicesProvider {

        @Override
        public void init(final StateProviderInitializer initializer) {
            initializer.computeAfterOpenDialog();
        }

        @Override
        public List<StringChoice> computeState(final NodeParametersInput context) {
            try {
                final PortObjectSpec[] inSpecs = context.getInPortSpecs();
                if (inSpecs.length == 0) {
                    throw new InvalidSettingsException(
                            "Missing input connection, Microsoft Fabric Workspace Connector required.");
                }

                if (inSpecs[0] instanceof FabricWorkspacePortObjectSpec) {
                    final FabricWorkspacePortObjectSpec spec = (FabricWorkspacePortObjectSpec) inSpecs[0];
                    final FabricConnection connection = spec.getFabricConnection();
                    final WarehouseAPI client = connection.getAPI(WarehouseAPI.class);

                    final List<Warehouse> warehouses = getAllWarehouses(client, connection.getWorkspaceId());
                    return warehouses.stream() //
                            .sorted(COMPARATOR) //
                            .map(i -> new StringChoice(i.id, i.displayName)) //
                            .collect(Collectors.toList());
                }

                throw new InvalidSettingsException(
                        "Invalid input connection, Microsoft Fabric Workspace Connector required.");
            } catch (final Exception e) { // NOSONAR catch all exceptions here
                LOGGER.info("Unable to fetch Microsoft Fabric Data Warehouses list.", e);
                throw new WidgetHandlerException(e.getMessage());
            }
        }

        private static List<Warehouse> getAllWarehouses(final WarehouseAPI client, final String workspaceId)
                throws IOException {
            final LinkedList<Warehouse> results = new LinkedList<>();
            String continuationToken = null;
            do {
                Warehouses warehouses = null;
                if (StringUtils.isEmpty(continuationToken)) {
                    warehouses = client.listWarehouses(workspaceId);
                } else {
                    warehouses = client.listWarehouses(workspaceId, continuationToken);
                }
                results.addAll(Arrays.asList(warehouses.warehouses));
                continuationToken = warehouses.continuationToken;
            } while (!StringUtils.isEmpty(continuationToken));
            return results;
        }
    }
}
