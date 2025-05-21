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
package org.knime.ext.azure.fabric.warehouse.node.io;

import static java.util.Arrays.asList;
import static org.knime.database.agent.metadata.DBMetaDataHelper.createDBTableSpec;
import static org.knime.datatype.mapping.DataTypeMappingDirection.KNIME_TO_EXTERNAL;

import java.sql.SQLType;
import java.util.List;

import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.JPanel;
import javax.swing.border.TitledBorder;

import org.knime.core.data.DataTableSpec;
import org.knime.core.data.convert.map.ConsumptionPath;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DialogComponent;
import org.knime.core.node.defaultnodesettings.SettingsModel;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.database.DBTableSpec;
import org.knime.database.datatype.mapping.DBTypeMappingRegistry;
import org.knime.database.extension.mssql.MSSQLServerDriverLocator;
import org.knime.database.model.DBTable;
import org.knime.database.node.io.load.DBLoaderNode2;
import org.knime.database.node.io.load.DBLoaderNode2Factory;
import org.knime.database.node.io.load.ExecutionParameters;
import org.knime.database.node.io.load.impl.unconnected.UnconnectedLoaderNode2;
import org.knime.database.port.DBDataPortObjectSpec;
import org.knime.database.port.DBPortObject;
import org.knime.database.session.DBSession;
import org.knime.datatype.mapping.DataTypeMappingConfiguration;

/**
 * Implementation of the loader node for the MySQL database.
 *
 * @author Tobias Koetter, KNIME GmbH, Konstanz, Germany
 */
public class MSSQLLoaderNode2 extends UnconnectedLoaderNode2<MSSQLLoaderNodeComponents2, MSSQLLoaderNodeSettings2>
    implements DBLoaderNode2Factory<MSSQLLoaderNodeComponents2, MSSQLLoaderNodeSettings2> {

    private static Box createBox(final boolean horizontal) {
        final Box box;
        if (horizontal) {
            box = new Box(BoxLayout.X_AXIS);
            box.add(Box.createVerticalGlue());
        } else {
            box = new Box(BoxLayout.Y_AXIS);
            box.add(Box.createHorizontalGlue());
        }
        return box;
    }

    private static JPanel createPanel() {
        final JPanel panel = new JPanel();
        panel.setLayout(new BoxLayout(panel, BoxLayout.Y_AXIS));
        return panel;
    }

    private static JPanel createTablePanel(final MSSQLLoaderNodeComponents2 customComponents) {
        final JPanel optionsPanel = createPanel();
        final Box optionsBox = createBox(false);
        optionsBox.setBorder(new TitledBorder("Target table"));
        optionsPanel.add(optionsBox);
        optionsBox.add(customComponents.getTableNameComponent().getComponentPanel());
        return optionsPanel;
    }

    @Override
    public DBLoaderNode2<MSSQLLoaderNodeComponents2, MSSQLLoaderNodeSettings2> get() {
        return new MSSQLLoaderNode2();
    }

    @Override
    public void buildDialog(final DialogBuilder builder, final List<DialogComponent> dialogComponents,
        final MSSQLLoaderNodeComponents2 customComponents) {
        final JPanel optionsPanel = createTablePanel(customComponents);
        builder.addTab(Integer.MAX_VALUE, "Options", optionsPanel, true);
        final JPanel advancedPanel = createPanel();
        final Box advancedBox = createBox(false);
        advancedPanel.add(advancedBox);
        advancedBox.add(customComponents.getLoaderComponent().getComponentPanel());
        builder.addTab(Integer.MAX_VALUE, "Advanced", advancedPanel, true);
    }

    @Override
    public MSSQLLoaderNodeSettings2 createCustomModelSettings(final ModelDelegate modelDelegate) {
        return new MSSQLLoaderNodeSettings2(modelDelegate);
    }

    @Override
    public MSSQLLoaderNodeComponents2 createCustomDialogComponents(final DialogDelegate dialogDelegate)
        throws NotConfigurableException {
        return new MSSQLLoaderNodeComponents2(dialogDelegate);
    }

    @Override
    public List<DialogComponent> createDialogComponents(final MSSQLLoaderNodeComponents2 customComponents) {
        return asList(customComponents.getTableNameComponent(), customComponents.getLoaderComponent());
    }

    @Override
    public List<SettingsModel> createSettingsModels(final MSSQLLoaderNodeSettings2 customSettings) {
        return asList(customSettings.getTableNameModel(), customSettings.getLoaderModel());
    }

    @Override
    public DBDataPortObjectSpec configureModel(final PortObjectSpec[] inSpecs,
        final List<SettingsModel> settingsModels, final MSSQLLoaderNodeSettings2 customSettings)
        throws InvalidSettingsException {
        final DBPortObject dbPortObject = getDBSpec(inSpecs);
        final DBSession dbSession = dbPortObject.getDBSession();
        if (MSSQLServerDriverLocator.DRIVER_ID.equals(dbSession.getDriver().getDriverDefinition().getId())) {
            throw new InvalidSettingsException("Official Microsoft JDBC driver required for bulk loading");
        }
        validateColumns(false, createModelConfigurationExecutionMonitor(dbSession),
            getDataSpec(inSpecs), dbPortObject, customSettings.getTableNameModel().toDBTable());
        return super.configureModel(inSpecs, settingsModels, customSettings);
    }

    @Override
    public DBTable load(final ExecutionParameters<MSSQLLoaderNodeSettings2> parameters) throws Exception {
        final DataTableSpec tableSpecification = parameters.getRowInput().getDataTableSpec();
        final DBPortObject sessionPortObject = parameters.getDBPortObject();
        final DBSession session = sessionPortObject.getDBSession();
        final ExecutionMonitor exec = parameters.getExecutionMonitor();
        exec.setMessage("Validating input columns");
        exec.checkCanceled();
        final MSSQLLoaderNodeSettings2 loaderSettings = parameters.getCustomSettings();
        final DBTable dbTable = loaderSettings.getTableNameModel().toDBTable();
        final DataTypeMappingConfiguration<SQLType> typeMappingConfiguration =
                sessionPortObject.getKnimeToExternalTypeMapping().resolve(DBTypeMappingRegistry.getInstance()
                    .getDBTypeMappingService(session.getDBType()), KNIME_TO_EXTERNAL);
        final ConsumptionPath[] consumptionPaths = typeMappingConfiguration.getConsumptionPathsFor(tableSpecification);
        final org.knime.database.model.DBColumn[] dbColumns = createDBTableSpec(tableSpecification, consumptionPaths);
        final DBTableSpec dbTableSpe = getDBTableSpecification(exec, dbTable, session);
        validateColumns(true, dbColumns, dbTableSpe);

        exec.setMessage("Upload data to database");
        return dbTable;
    }

}
