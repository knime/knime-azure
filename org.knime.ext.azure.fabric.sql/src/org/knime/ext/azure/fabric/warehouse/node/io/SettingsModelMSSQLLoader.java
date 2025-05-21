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

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.config.Config;
import org.knime.core.node.defaultnodesettings.SettingsModel;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.database.node.component.dbrowser.SettingsModelDBMetadata;

/**
 * {@link SettingsModel} that stores all the {@code com.microsoft.sqlserver.jdbc.SQLServerBulkCopyOptions} values.
 *
 * @author Tobias Koetter, KNIME GmbH, Konstanz, Germany
 */
public class SettingsModelMSSQLLoader extends SettingsModel {

    private final String m_configName;

    private static final int DEFAULT_BATCH_SIZE = 0;
    private static final String CFG_BATCH_SIZE = "batchSize";
    private int m_batchSize = DEFAULT_BATCH_SIZE;

    private static final int DEFAULT_BULK_COPY_TIMEOUT = 60;
    private static final String CFG_BULK_COPY_TIMEOUT = "bulkCopyTimeout";
    private int m_bulkCopyTimeout = DEFAULT_BULK_COPY_TIMEOUT;

    private static final String CFG_CHECK_CONSTRAINTS = "checkConstraints";
    private boolean m_checkConstraints = false;

    private static final String CFG_FIRE_TRIGGERS = "fireTriggers";
    private boolean m_fireTriggers = false;

    private static final String CFG_KEEP_IDENTIY = "keepIdentity";
    private boolean m_keepIdentity = false;

    private static final String CFG_KEEP_NULLS = "keepNulls";
    private boolean m_keepNulls = false;

    private static final String CFG_TABLE_LOCK = "tableLock";
    private boolean m_tableLock = true;

    /*!!!useInternalTransaction is not supported when using an existing connection in the SQLServerBulkCopy class!!!*/

    private static final String CFG_ALLOW_ENCRYPTED_VALUE_MODIFICATIONS = "allowEncryptedValueModifications";
    private boolean m_allowEncryptedValueModifications = false;



    /**
     * Constructs a {@link SettingsModelDBMetadata} object.
     *
     * @param configName the key of the configuration element containing the data of the settings model.
     */
    public SettingsModelMSSQLLoader(final String configName) {
        m_configName = configName;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected SettingsModelMSSQLLoader createClone() {
        final SettingsModelMSSQLLoader clone = new SettingsModelMSSQLLoader(m_configName);
        clone.m_batchSize = m_batchSize;
        clone.m_bulkCopyTimeout = m_bulkCopyTimeout;
        clone.m_checkConstraints = m_checkConstraints;
        clone.m_fireTriggers = m_fireTriggers;
        clone.m_keepIdentity = m_keepIdentity;
        clone.m_keepNulls = m_keepNulls;
        clone.m_tableLock = m_tableLock;
        clone.m_allowEncryptedValueModifications = m_allowEncryptedValueModifications;
        return clone;
    }

    @Override
    protected void validateSettingsForModel(final NodeSettingsRO settings) throws InvalidSettingsException {
        final Config config = settings.getConfig(m_configName);
        config.getInt(CFG_BATCH_SIZE);
        config.getInt(CFG_BULK_COPY_TIMEOUT);
        config.getBoolean(CFG_CHECK_CONSTRAINTS);
        config.getBoolean(CFG_FIRE_TRIGGERS);
        config.getBoolean(CFG_KEEP_IDENTIY);
        config.getBoolean(CFG_KEEP_NULLS);
        config.getBoolean(CFG_TABLE_LOCK);
        config.getBoolean(CFG_ALLOW_ENCRYPTED_VALUE_MODIFICATIONS);
    }

    @Override
    protected void loadSettingsForModel(final NodeSettingsRO settings) throws InvalidSettingsException {
        final Config config = settings.getConfig(m_configName);
        m_batchSize = config.getInt(CFG_BATCH_SIZE);
        m_bulkCopyTimeout = config.getInt(CFG_BULK_COPY_TIMEOUT);
        m_checkConstraints = config.getBoolean(CFG_CHECK_CONSTRAINTS);
        m_fireTriggers = config.getBoolean(CFG_FIRE_TRIGGERS);
        m_keepIdentity = config.getBoolean(CFG_KEEP_IDENTIY);
        m_keepNulls = config.getBoolean(CFG_KEEP_NULLS);
        m_tableLock = config.getBoolean(CFG_TABLE_LOCK);
        m_allowEncryptedValueModifications = config.getBoolean(CFG_ALLOW_ENCRYPTED_VALUE_MODIFICATIONS);
    }

    @Override
    protected void saveSettingsForModel(final NodeSettingsWO settings) {
        final Config config = settings.addConfig(m_configName);
        config.addInt(CFG_BATCH_SIZE, m_batchSize);
        config.addInt(CFG_BULK_COPY_TIMEOUT, m_bulkCopyTimeout);
        config.addBoolean(CFG_CHECK_CONSTRAINTS, m_checkConstraints);
        config.addBoolean(CFG_FIRE_TRIGGERS, m_fireTriggers);
        config.addBoolean(CFG_KEEP_IDENTIY, m_keepIdentity);
        config.addBoolean(CFG_KEEP_NULLS, m_keepNulls);
        config.addBoolean(CFG_TABLE_LOCK, m_tableLock);
        config.addBoolean(CFG_ALLOW_ENCRYPTED_VALUE_MODIFICATIONS, m_allowEncryptedValueModifications);
    }

    /**
     * Set the schema and table values.
     * @param batchSize the batch size
     * @param bulkCopyTimeout the bulk copy timeout
     * @param checkConstraints {@code true} to check constraints
     * @param fireTriggers {@code true} to fire triggers
     * @param tableLock {@code true} to use table locking
     * @param keepIdentity {@code true} to keep the identity values
     * @param keepNulls {@code true} to keep nulls
     * @param allowEncryptedValueModifications {@code true} to allow encrypted value modification
     */
    public void setValues(final int batchSize, final int bulkCopyTimeout, final boolean checkConstraints,
        final boolean fireTriggers, final boolean keepIdentity, final boolean keepNulls,
        final boolean tableLock, final boolean allowEncryptedValueModifications) {
        boolean changed = false;
        changed = setBatchSize(batchSize) || changed;
        changed = setBulkCopyTimeout(bulkCopyTimeout) || changed;
        changed = setCheckConstraints(checkConstraints) || changed;
        changed = setFireTriggers(fireTriggers) || changed;
        changed = setKeepIdentity(keepIdentity) || changed;
        changed = setKeepNulls(keepNulls) || changed;
        changed = setTableLock(tableLock) || changed;
        changed = setAllowEncryptedValueModifications(allowEncryptedValueModifications) || changed;
        if (changed) {
            notifyChangeListeners();
        }
    }

    private boolean setBatchSize(final int value) {
        if (value == m_batchSize) {
            return false;
        }
        m_batchSize = value;
        return true;
    }

    private boolean setBulkCopyTimeout(final int value) {
        if (value == m_bulkCopyTimeout) {
            return false;
        }
        m_bulkCopyTimeout = value;
        return true;
    }

    private boolean setCheckConstraints(final boolean value) {
        if (value == m_checkConstraints) {
            return false;
        }
        m_checkConstraints = value;
        return true;
    }

    private boolean setFireTriggers(final boolean value) {
        if (value == m_fireTriggers) {
            return false;
        }
        m_fireTriggers = value;
        return true;
    }

    private boolean setKeepIdentity(final boolean value) {
        if (value == m_keepIdentity) {
            return false;
        }
        m_keepIdentity = value;
        return true;
    }

    private boolean setKeepNulls(final boolean value) {
        if (value == m_keepNulls) {
            return false;
        }
        m_keepNulls = value;
        return true;
    }

    private boolean setTableLock(final boolean value) {
        if (value == m_tableLock) {
            return false;
        }
        m_tableLock = value;
        return true;
    }

    private boolean setAllowEncryptedValueModifications(final boolean value) {
        if (value == m_allowEncryptedValueModifications) {
            return false;
        }
        m_allowEncryptedValueModifications = value;
        return true;
    }

    /**
     * Returns the batch size.
     *
     * @return the batchSize
     */
    public int getBatchSize() {
        return m_batchSize;
    }

    /** Returns the bulk copy timeout.
     *
     * @return the bulkCopyTimeout
     */
    public int getBulkCopyTimeout() {
        return m_bulkCopyTimeout;
    }

    /**
     * Returns check constraints.
     *
     * @return check constraints
     */
    public boolean isCheckConstraints() {
        return m_checkConstraints;
    }

    /**
     * Returns fire trigger.
     *
     * @return fire trigger
     */
    public boolean isFireTriggers() {
        return m_fireTriggers;
    }

    /**
     * Returns the keep identity value.
     *
     * @return the keepIdentity
     */
    public boolean isKeepIdentity() {
        return m_keepIdentity;
    }

    /**
     * Returns keep nulls.
     *
     * @return keep nulls
     */
    public boolean isKeepNulls() {
        return m_keepNulls;
    }

    /**
     * Returns the table lock value.
     *
     * @return the tableLock
     */
    public boolean isTableLock() {
        return m_tableLock;
    }

    /**
     * Returns allow encrypted value modifications.
     *
     * @return allow encrypted value modifications
     */
    public boolean isAllowEncryptedValueModifications() {
        return m_allowEncryptedValueModifications;
    }

    @Override
    protected String getModelTypeID() {
        return "SMID_mssqlloader";
    }

    @Override
    protected String getConfigName() {
        return m_configName;
    }

    @Override
    protected void loadSettingsForDialog(final NodeSettingsRO settings, final PortObjectSpec[] specs)
        throws NotConfigurableException {
        // Does not prevent the dialog from opening if the configuration is missing.
        if (settings.containsKey(m_configName)) {
            try {
                loadSettingsForModel(settings);
            } catch (InvalidSettingsException ex) {
                throw new NotConfigurableException(ex.getMessage(), ex);
            }
        }
    }

    @Override
    protected void saveSettingsForDialog(final NodeSettingsWO settings) throws InvalidSettingsException {
        saveSettingsForModel(settings);
    }

    @Override
    public String toString() {
        final StringBuilder builder = new StringBuilder();
        builder.append("SettingsModelMSSQLLoader [m_configName=");
        builder.append(m_configName);
        builder.append(", m_batchSize=");
        builder.append(m_batchSize);
        builder.append(", m_bulkCopyTimeout=");
        builder.append(m_bulkCopyTimeout);
        builder.append(", m_checkConstraints=");
        builder.append(m_checkConstraints);
        builder.append(", m_fireTriggers=");
        builder.append(m_fireTriggers);
        builder.append(", m_keepIdentity=");
        builder.append(m_keepIdentity);
        builder.append(", m_keepNulls=");
        builder.append(m_keepNulls);
        builder.append(", m_tableLock=");
        builder.append(m_tableLock);
        builder.append(", m_allowEncryptedValueModifications=");
        builder.append(m_allowEncryptedValueModifications);
        builder.append("]");
        return builder.toString();
    }

}
