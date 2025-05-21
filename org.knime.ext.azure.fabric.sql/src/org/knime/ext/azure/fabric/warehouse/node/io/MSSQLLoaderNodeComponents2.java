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

import static java.util.Objects.requireNonNull;

import org.knime.database.node.component.dbrowser.DBTableSelectorDialogComponent;
import org.knime.database.node.component.dbrowser.SettingsModelDBMetadata;
import org.knime.database.node.component.dbrowser.VariableModelCreator;
import org.knime.database.node.io.load.DBLoaderNode2.DialogDelegate;

/**
 * Node dialog components and corresponding settings for {@link MSSQLLoaderNode}.
 *
 * @author Tobias Koetter, KNIME GmbH, Konstanz, Germany
 */
public class MSSQLLoaderNodeComponents2 {

    private final DialogDelegate m_dialogDelegate;

    private final DBTableSelectorDialogComponent m_tableNameComponent;

    private final SettingsModelDBMetadata m_tableNameModel;

    private final MSSQLLoaderDialogComponent m_loaderComponent;

    private final SettingsModelMSSQLLoader m_loaderModel;

    /**
     * Constructs an {@link MSSQLLoaderNodeComponents2} object.
     *
     * @param dialogDelegate the delegate of the node dialog to create components for.
     */
    public MSSQLLoaderNodeComponents2(final DialogDelegate dialogDelegate) {
        m_dialogDelegate = requireNonNull(dialogDelegate, "dialogDelegate");
        m_tableNameModel = createTableNameModel();
        m_tableNameComponent = createTableNameComponent(m_tableNameModel, dialogDelegate);
        m_loaderModel = createLoaderModel();
        m_loaderComponent = createLoaderComponent(m_loaderModel);
    }

    /**
     * Gets the delegate of the node dialog the components have been created for.
     *
     * @return a {@link DialogDelegate} object.
     */
    public DialogDelegate getDialogDelegate() {
        return m_dialogDelegate;
    }

    /**
     * Gets the database table name dialog component.
     *
     * @return a {@link DBTableSelectorDialogComponent} object.
     */
    public DBTableSelectorDialogComponent getTableNameComponent() {
        return m_tableNameComponent;
    }

    /**
     * Gets the database table name settings model.
     *
     * @return a {@link SettingsModelDBMetadata} object.
     */
    public SettingsModelDBMetadata getTableNameModel() {
        return m_tableNameModel;
    }

    /**
     * Returns the {@link MSSQLLoaderDialogComponent}.
     *
     * @return the loaderComponent
     */
    public MSSQLLoaderDialogComponent getLoaderComponent() {
        return m_loaderComponent;
    }

    /**
     * Returns the {@link SettingsModelMSSQLLoader}.
     *
     * @return the loaderModel
     */
    public SettingsModelMSSQLLoader getLoaderModel() {
        return m_loaderModel;
    }

    /**
     * Creates the table name component.
     *
     * @param tableNameModel the already created table name settings model.
     * @param delegate {@link DialogDelegate}
     * @return a {@link DBTableSelectorDialogComponent} object.
     */
    protected DBTableSelectorDialogComponent createTableNameComponent(final SettingsModelDBMetadata tableNameModel,
        final DialogDelegate delegate) {
        return new DBTableSelectorDialogComponent(m_tableNameModel, 1, false, null, "Select a table",
            "Database Metadata Browser", true,
            (VariableModelCreator<String>)(keys, type) -> delegate.getDialog().createFlowVariableModel(keys, type));
    }

    /**
     * Creates the table name settings model.
     *
     * @return a {@link SettingsModelDBMetadata} object.
     */
    protected SettingsModelDBMetadata createTableNameModel() {
        return new SettingsModelDBMetadata("tableName");
    }

    /**
     * Creates the {@link MSSQLLoaderDialogComponent}.
     *
     * @param loaderModel the {@link SettingsModelMSSQLLoader}
     * @return the {@link MSSQLLoaderDialogComponent}
     */
    protected MSSQLLoaderDialogComponent createLoaderComponent(final SettingsModelMSSQLLoader loaderModel) {
        return new MSSQLLoaderDialogComponent(loaderModel);
    }

    /**
     * Returns the {@link SettingsModelMSSQLLoader} model.
     *
     * @return the {@link SettingsModelMSSQLLoader}
     */
    protected SettingsModelMSSQLLoader createLoaderModel() {
        return new SettingsModelMSSQLLoader("mssqlLoader");
    }
}
