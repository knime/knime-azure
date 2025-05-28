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

package org.knime.ext.azure.fabric.warehouse.dialect;

import java.util.Objects;

import org.knime.database.attribute.AttributeCollection;
import org.knime.database.dialect.DBSQLDialect;
import org.knime.database.dialect.DBSQLDialectFactory;
import org.knime.database.dialect.DBSQLDialectFactoryParameters;
import org.knime.database.dialect.DBSQLDialectParameters;
import org.knime.database.extension.mssql.dialect.MSSQLServerDBSQLDialect;

/**
 * {@link DBSQLDialect} for Microsoft Fabric Data Warehouses.
 *
 * @author Tobias Koetter, KNIME GmbH, Konstanz, Germany
 */
public class FabricWarhouseSQLDialect extends MSSQLServerDBSQLDialect {
    /**
     * {@link DBSQLDialectFactory} that produces {@link FabricWarhouseSQLDialect}
     * instances.
     *
     * @author Noemi Balassa
     */
    public static class Factory implements DBSQLDialectFactory {
        @Override
        public DBSQLDialect createDialect(final DBSQLDialectFactoryParameters parameters) {
            return new FabricWarhouseSQLDialect(this,
                    new DBSQLDialectParameters(Objects.requireNonNull(parameters, "parameters").getSessionReference()));
        }

        @Override
        public AttributeCollection getAttributes() {
            return ATTRIBUTES;
        }

        @Override
        public String getDescription() {
            return DESCRIPTION;
        }

        @Override
        public String getId() {
            return ID;
        }

        @Override
        public String getName() {
            return NAME;
        }
    }

    /**
     * The {@linkplain #getId() ID} of the {@link MSSQLServerDBSQLDialect}
     * instances.
     *
     * @see DBSQLDialectFactory#getId()
     * @see FabricWarhouseSQLDialect.Factory#getId()
     */
    @SuppressWarnings("hiding")
    public static final String ID = "fabricwarehouse";

    /**
     * The {@linkplain #getDescription() description} of the
     * {@link MSSQLServerDBSQLDialect} instances.
     *
     * @see DBSQLDialectFactory#getDescription()
     * @see MSSQLServerDBSQLDialect.Factory#getDescription()
     */
    static final String DESCRIPTION = "Microsoft Fabric Data Warehouse";

    /**
     * The {@linkplain #getName() name} of the {@link MSSQLServerDBSQLDialect}
     * instances.
     *
     * @see DBSQLDialectFactory#getName()
     * @see MSSQLServerDBSQLDialect.Factory#getName()
     */
    static final String NAME = "Microsoft Fabric Data Warehouse";

    /**
     * Constructs an {@link MSSQLServerDBSQLDialect} object.
     *
     * @param factory
     *            the factory that produces the instance.
     * @param dialectParameters
     *            the dialect-specific parameters controlling statement creation.
     */
    protected FabricWarhouseSQLDialect(final DBSQLDialectFactory factory,
            final DBSQLDialectParameters dialectParameters) {
        super(factory, dialectParameters);
    }
}
