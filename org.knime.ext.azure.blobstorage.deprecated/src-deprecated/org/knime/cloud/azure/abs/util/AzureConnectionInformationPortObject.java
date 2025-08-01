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
 *   Aug 22, 2016 (oole): created
 */
package org.knime.cloud.azure.abs.util;

import javax.swing.JComponent;

import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformationPortObject;
import org.knime.cloud.core.util.port.CloudConnectionInformationPortObjectSpec;
import org.knime.core.node.port.PortType;

/**
 *
 * @author Ole Ostergaard, KNIME.com GmbH
 */
@Deprecated
public class AzureConnectionInformationPortObject extends ConnectionInformationPortObject {

	/**
     * @noreference This class is not intended to be referenced by clients.
     */
    public static final class Serializer extends AbstractSimplePortObjectSerializer<AzureConnectionInformationPortObject> {}

    /**
	 * The port type
	 */
	public static final PortType TYPE = ConnectionInformationPortObject.TYPE;

	public static final PortType TYPE_OPTIONAL = ConnectionInformationPortObject.TYPE_OPTIONAL;

	/**
	 * The constructor.
	 * @param connectionInformationPortObjectSpec spec
	 * @deprecated
	 */
	@Deprecated
    public AzureConnectionInformationPortObject(final CloudConnectionInformationPortObjectSpec connectionInformationPortObjectSpec) {
		super(connectionInformationPortObjectSpec);
	}

	/**
	 * No-argument constructor
	 * @deprecated
	 */
	@Deprecated
    public AzureConnectionInformationPortObject() {
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public JComponent[] getViews() {
		return new JComponent[] {new AzureConnectionInformationView(getConnectionInformation())};
	}
}