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

package org.knime.ext.azure.fabric.warehouse;

import org.knime.database.agent.DBAgentFactory;
import org.knime.database.agent.loader.DBLoader;
import org.knime.database.agent.metadata.DBMetadataReader;
import org.knime.database.agent.metadata.impl.DefaultDBMetadataReader;
import org.knime.database.attribute.AttributeCollection;
import org.knime.database.attribute.AttributeCollection.Accessibility;
import org.knime.database.extension.mssql.MSSQLAgentFactory;
import org.knime.ext.azure.fabric.warehouse.agent.FabricWarehouseLoader;

/**
 * {@linkplain DBAgentFactory Agent factory} for the Microsoft Fabric Data
 * Warehouse.
 *
 * @author Tobias Koetter, KNIME GmbH, Konstanz, Germany
 */
public class FabricWarehouseAgentFactory extends MSSQLAgentFactory {

    private static final AttributeCollection METADATA_ATTRIBUTES;
    static {
        final AttributeCollection.Builder builder = AttributeCollection.builder(DefaultDBMetadataReader.ATTRIBUTES);
        builder.add(Accessibility.EDITABLE, DefaultDBMetadataReader.ATTRIBUTE_CAPABILITY_MULTI_DBS, false);
        METADATA_ATTRIBUTES = builder.build();
    }
    //
    // private static final AttributeCollection SAMPLING_ATTRIBUTES;
    // static {
    // final AttributeCollection.Builder builder =
    // AttributeCollection.builder(DefaultDBSampling.ATTRIBUTES);
    // builder.add(Accessibility.READ_ONLY,
    // DefaultDBSampling.ATTRIBUTE_CAPABILITY_RANDOM, true);
    // SAMPLING_ATTRIBUTES = builder.build();
    // }

    /**
     * Constructs a {@link FabricWarehouseAgentFactory}.
     */
    public FabricWarehouseAgentFactory() {
        super();
        putAttributes(DBMetadataReader.class, METADATA_ATTRIBUTES);
        putCreator(DBMetadataReader.class,
                parameters -> new DefaultDBMetadataReader(parameters.getSessionReference()));
        // putAttributes(DBSampling.class, SAMPLING_ATTRIBUTES);
        // putCreator(DBSampling.class, parameters -> new
        // MSSQLServerDBSampling(parameters.getSessionReference()));
        // putCreator(DBStructureManipulator.class,
        // parameters -> new
        // DefaultDBStructureManipulator(parameters.getSessionReference()));
        putCreator(DBLoader.class, parameters -> new FabricWarehouseLoader(parameters.getSessionReference()));
    }

}
