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

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;

import javax.swing.JCheckBox;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JSpinner;
import javax.swing.SpinnerNumberModel;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DialogComponent;
import org.knime.core.node.port.PortObjectSpec;

/**
 * Dialog component for specifying the MS SQL Server loader options.
 *
 * @author Tobias Koetter, KNIME GmbH, Konstanz, Germany
 */
public class MSSQLLoaderDialogComponent extends DialogComponent {

    private final JSpinner m_batchSize;
    private final JSpinner m_bulkCopyTimeout;
    private final JCheckBox m_checkConstraints;
    private final JCheckBox m_fireTriggers;
    private final JCheckBox m_keepIdentiy;
    private final JCheckBox m_keepNulls;
    private final JCheckBox m_tableLock;
    private final JCheckBox m_allowEncryptedValueModifications;


    /**
     * Constructs a {@link MSSQLLoaderDialogComponent}.
     *
     * @param model the settings model of the dialog component.
     */
    public MSSQLLoaderDialogComponent(
            final org.knime.ext.azure.fabric.warehouse.node.io.SettingsModelMSSQLLoader model) {
        super(model);
        m_batchSize = new JSpinner(new SpinnerNumberModel(0, 0, Integer.MAX_VALUE, 100));
        m_batchSize.setToolTipText("Number of rows in each batch that is send to the server. "
            + "0 - indicates a single batch.");

        m_bulkCopyTimeout = new JSpinner(new SpinnerNumberModel(60, 1, Integer.MAX_VALUE, 10));
        m_bulkCopyTimeout.setToolTipText("Number of seconds for the operation to complete before it times out. "
            + "A value of 0 indicates no limit; the bulk copy will wait indefinitely.");

        m_checkConstraints = new JCheckBox("Check constraints");
        m_checkConstraints.setToolTipText("Check constraints while data is being inserted.");

        m_fireTriggers = new JCheckBox("Fire triggers");
        m_fireTriggers.setToolTipText(
            "Cause the server to fire the insert triggers for the rows being inserted into the database.");

        m_keepIdentiy = new JCheckBox("Keep identity values");
        m_keepIdentiy.setToolTipText("Preserve source identity values. "
            + "False - identity values are assigned by the destination.");

        m_keepNulls = new JCheckBox("Keep nulls");
        m_keepNulls.setToolTipText(
            "Preserve null values in the destination table regardless of the settings for default values.");

        m_tableLock = new JCheckBox("Use table lock");
        m_tableLock.setToolTipText("Obtain a bulk update lock for the operation. Otherwise row locks are used.");

        m_allowEncryptedValueModifications = new JCheckBox("Allow encrypted value modifications");
        m_allowEncryptedValueModifications.setToolTipText(
            "Enables bulk copying of encrypted data between tables or databases, without decrypting the data.");

        initialize();
    }


    @Override
    public void setToolTipText(final String text) {
        getComponentPanel().setToolTipText(text);
    }

    @Override
    protected void checkConfigurabilityBeforeLoad(final PortObjectSpec[] specs) throws NotConfigurableException {
    }

    @Override
    protected void setEnabledComponents(final boolean enabled) {
        m_batchSize.setEnabled(enabled);
        m_bulkCopyTimeout.setEnabled(enabled);
        m_checkConstraints.setEnabled(enabled);
        m_fireTriggers.setEnabled(enabled);
        m_keepIdentiy.setEnabled(enabled);
        m_keepNulls.setEnabled(enabled);
        m_tableLock.setEnabled(enabled);
        m_allowEncryptedValueModifications.setEnabled(enabled);
    }

    @Override
    protected void updateComponent() {
        final SettingsModelMSSQLLoader model = (SettingsModelMSSQLLoader)getModel();
        m_batchSize.setValue(model.getBatchSize());
        m_bulkCopyTimeout.setValue(model.getBulkCopyTimeout());
        m_checkConstraints.setSelected(model.isCheckConstraints());
        m_fireTriggers.setSelected(model.isFireTriggers());
        m_keepIdentiy.setSelected(model.isKeepIdentity());
        m_keepNulls.setSelected(model.isKeepNulls());
        m_tableLock.setSelected(model.isTableLock());
        m_allowEncryptedValueModifications.setSelected(model.isAllowEncryptedValueModifications());
        setEnabledComponents(model.isEnabled());
    }

    @Override
    protected void validateSettingsBeforeSave() throws InvalidSettingsException {
        updateModel();
    }

    private void initialize() {
        // Layout
        final JPanel formPanel = new JPanel(new GridBagLayout());
        final GridBagConstraints gbc = new GridBagConstraints();
        gbc.gridx = 0;
        gbc.gridy = 0;
        gbc.weightx = 1;
        gbc.anchor = GridBagConstraints.LINE_START;
        gbc.fill = GridBagConstraints.HORIZONTAL;
        gbc.gridwidth = 2;
        formPanel.add(m_checkConstraints, gbc);

        gbc.gridy++;
        formPanel.add(m_fireTriggers, gbc);

        gbc.gridy++;
        formPanel.add(m_keepIdentiy, gbc);

        gbc.gridy++;
        formPanel.add(m_keepNulls, gbc);

        gbc.gridy++;
        formPanel.add(m_tableLock, gbc);

//NOT TESTED AND MIGHT NOT WORK BECAUSE OF COLUMN VALIDATION
//        gbc.gridy++;
//        formPanel.add(m_allowEncryptedValueModifications, gbc);

        gbc.gridy++;
        gbc.gridwidth = 1;
        gbc.weightx = 0;
        formPanel.add(new JLabel("Batch Size: "), gbc);
        gbc.gridx++;
        gbc.weightx = 1;
        formPanel.add(m_batchSize, gbc);

        gbc.gridy++;
        gbc.gridx = 0;
        gbc.gridwidth = 1;
        gbc.weightx = 0;
        formPanel.add(new JLabel("Bulk copy timeout: "), gbc);
        gbc.gridx++;
        gbc.weightx = 1;
        formPanel.add(m_bulkCopyTimeout, gbc);

        getComponentPanel().add(formPanel);
        updateComponent();
    }

    private void updateModel() {
        final SettingsModelMSSQLLoader settings = (SettingsModelMSSQLLoader)getModel();
        settings.setValues((int)m_batchSize.getValue(), (int)m_bulkCopyTimeout.getValue(),
            m_checkConstraints.isSelected(), m_fireTriggers.isSelected(), m_keepIdentiy.isSelected(),
            m_keepNulls.isSelected(), m_tableLock.isSelected(), m_allowEncryptedValueModifications.isSelected());
    }

}
