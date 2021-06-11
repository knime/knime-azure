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
 *   2021-01-30 (Alexander Bondaletov): created
 */
package org.knime.ext.azure.adls.gen2.filehandling.node;

import java.time.Duration;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelIntegerBounded;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.ext.azure.adls.gen2.filehandling.fs.AdlsFSConnectionConfig;
import org.knime.ext.azure.adls.gen2.filehandling.fs.AdlsFileSystem;
import org.knime.ext.microsoft.authentication.port.MicrosoftCredential;

/**
 * Settings for Adls connector node.
 *
 * @author Alexander Bondaletov
 */
final class AdlsConnectorSettings {
    private static final String KEY_WORKING_DIRECTORY = "workingDirectory";
    private static final String KEY_TIMEOUT = "timeout";

    private final SettingsModelString m_workingDirectory;
    private final SettingsModelIntegerBounded m_timeout;

    /**
     * Creates new instance.
     */
    public AdlsConnectorSettings() {
        m_workingDirectory = new SettingsModelString(KEY_WORKING_DIRECTORY, AdlsFileSystem.PATH_SEPARATOR);
        m_timeout = new SettingsModelIntegerBounded(KEY_TIMEOUT, AdlsFSConnectionConfig.DEFAULT_TIMEOUT, 0, Integer.MAX_VALUE);
    }

    /**
     * Saves the settings to the given {@link NodeSettingsWO}.
     *
     * @param settings
     *            The settings.
     */
    public void saveSettingsTo(final NodeSettingsWO settings) {
        m_workingDirectory.saveSettingsTo(settings);
        m_timeout.saveSettingsTo(settings);
    }

    /**
     * Validates settings in the given {@link NodeSettingsRO}.
     *
     * @param settings
     *            The settings.
     * @throws InvalidSettingsException
     */
    public void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_workingDirectory.validateSettings(settings);
        m_timeout.validateSettings(settings);

        AdlsConnectorSettings temp = new AdlsConnectorSettings();
        temp.loadSettingsFrom(settings);
        temp.validate();
    }

    /**
     * Validates settings consistency for this instance.
     *
     * @throws InvalidSettingsException
     */
    public void validate() throws InvalidSettingsException {
        String workDir = m_workingDirectory.getStringValue();
        if (workDir.isEmpty() || !workDir.startsWith(AdlsFileSystem.PATH_SEPARATOR)) {
            throw new InvalidSettingsException("Working directory must be set to an absolute path.");
        }
    }

    /**
     * Loads settings from the given {@link NodeSettingsRO}.
     *
     * @param settings
     *            The settings.
     * @throws InvalidSettingsException
     */
    public void loadSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_workingDirectory.loadSettingsFrom(settings);
        m_timeout.loadSettingsFrom(settings);
    }

    /**
     * @return the workingDirectory model.
     */
    public SettingsModelString getWorkingDirectoryModel() {
        return m_workingDirectory;
    }

    /**
     * @return the working directory
     */
    public String getWorkingDirectory() {
        return m_workingDirectory.getStringValue();
    }

    /**
     * @return the timeout model
     */
    public SettingsModelIntegerBounded getTimeoutModel() {
        return m_timeout;
    }

    /**
     * @return the timeout
     */
    public Duration getTimeout() {
        return Duration.ofSeconds(m_timeout.getIntValue());
    }

    /**
     *
     * @param credential
     *            The {@link MicrosoftCredential} to use.
     * @return The FSConnectionConfig for Azure data lake
     */
    public AdlsFSConnectionConfig toFSConnectionConfig(final MicrosoftCredential credential) {
        var config = new AdlsFSConnectionConfig(getWorkingDirectory());
        config.setCredential(credential);
        config.setTimeout(getTimeout());
        return config;

    }
}
