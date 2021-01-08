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
 *   2020-12-16 (Alexander Bondaletov): created
 */
package org.knime.ext.azure.adls.gen2.filehandling;

import org.eclipse.ui.plugin.AbstractUIPlugin;
import org.knime.core.node.NodeLogger;
import org.osgi.framework.BundleContext;

import com.azure.core.http.HttpClientProvider;
import com.azure.core.implementation.http.HttpClientProviders;

/**
 * Plugin activator for the ADLS plugin.
 *
 * @author Alexander Bondaletov
 */
public class AdlsPlugin extends AbstractUIPlugin {
    private static final NodeLogger LOG = NodeLogger.getLogger(AdlsPlugin.class);

    // The shared instance.
    private static AdlsPlugin plugin;

    /**
     * The constructor.
     */
    public AdlsPlugin() {
        plugin = this; // NOSONAR standard KNIME pattern
    }

    /**
     * This method is called upon plug-in activation.
     *
     * @param context
     *            The bundle context.
     * @throws Exception
     *             If cause by super class.
     */
    @Override
    public void start(final BundleContext context) throws Exception {
        super.start(context);

        final ClassLoader cl = Thread.currentThread().getContextClassLoader();
        try {
            // To talk to ADLS we need a
            // com.azure.core.http.HttpClientProvider, which is an interface
            // from azure-core. HttpClientProviders uses the ServiceLoader framework to
            // locate an implementation of this interface. The ServiceLoader framework tries
            // to find a suitable implementation from the TCCL, which we need to set
            // accordingly here, otherwise the no implementation class can be found.
            Thread.currentThread().setContextClassLoader(HttpClientProvider.class.getClassLoader());
            HttpClientProviders.createInstance();
        } catch (Exception e) { // NOSONAR we must catch all exceptions here as we don't know what might be
                                // thrown
            LOG.error("Failed to load ADLS client", e);
        } finally {
            Thread.currentThread().setContextClassLoader(cl);
        }
    }

    /**
     * This method is called when the plug-in is stopped.
     *
     * @param context
     *            The bundle context.
     * @throws Exception
     *             If cause by super class.
     */
    @Override
    public void stop(final BundleContext context) throws Exception {
        plugin = null; // NOSONAR standard KNIME pattern
        super.stop(context);
    }

    /**
     * Returns the shared instance.
     *
     * @return The shared instance
     */
    public static AdlsPlugin getDefault() {
        return plugin;
    }
}
