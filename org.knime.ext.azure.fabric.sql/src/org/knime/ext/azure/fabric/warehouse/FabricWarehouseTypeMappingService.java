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

import java.io.InputStream;
import java.sql.JDBCType;
import java.sql.SQLType;
import java.util.Map;

import org.apache.commons.lang3.tuple.Triple;
import org.knime.core.data.DataType;
import org.knime.core.data.blob.BinaryObjectDataCell;
import org.knime.core.data.time.zoneddatetime.ZonedDateTimeCellFactory;
import org.knime.database.datatype.mapping.AbstractDBDataTypeMappingService;
import org.knime.database.extension.mssql.MSSQLServerDestination;
import org.knime.database.extension.mssql.MSSQLServerSource;
import org.knime.database.extension.mssql.MSSQLServerTypeMappingService;

/**
 * Database type mapping service for Microsoft Fabric Data Warehouse.
 *
 * @author Tobias Koetter, KNIME GmbH, Konstanz, Germany
 */
public final class FabricWarehouseTypeMappingService
        extends AbstractDBDataTypeMappingService<MSSQLServerSource, MSSQLServerDestination> {

    private static final FabricWarehouseTypeMappingService INSTANCE = new FabricWarehouseTypeMappingService();

    /**
     * Gets the singleton {@link MSSQLServerTypeMappingService} instance.
     *
     * @return the only {@link MSSQLServerTypeMappingService} instance.
     */
    public static FabricWarehouseTypeMappingService getInstance() {
        return INSTANCE;
    }

    private FabricWarehouseTypeMappingService() {
        super(MSSQLServerSource.class, MSSQLServerDestination.class);

        // Default consumption paths
        final Map<DataType, Triple<DataType, Class<?>, SQLType>> defaultConsumptionMap = getDefaultConsumptionTriples();
        addTriple(defaultConsumptionMap, BinaryObjectDataCell.TYPE, InputStream.class, JDBCType.LONGVARBINARY);
        addTriple(defaultConsumptionMap, ZonedDateTimeCellFactory.TYPE, String.class, JDBCType.VARCHAR);
        setDefaultConsumptionTriples(defaultConsumptionMap);

        // Default production paths
        setDefaultProductionTriples(getDefaultProductionTriples());

        // See https://learn.microsoft.com/en-us/fabric/data-warehouse/data-types
        addColumnType(JDBCType.DOUBLE, "float(53)");
        addColumnType(JDBCType.VARCHAR, "varchar(max)");
        addColumnType(JDBCType.NVARCHAR, "varchar(max)");
        addColumnType(JDBCType.NCHAR, "varchar(max)");
        addColumnType(JDBCType.LONGNVARCHAR, "varchar(max)");
        addColumnType(JDBCType.LONGVARCHAR, "varchar(max)");
        // see https://docs.microsoft.com/en-us/sql/connect/jdbc/using-advanced-data-types?view=sql-server-2017
        addColumnType(JDBCType.LONGVARBINARY, "varbinary(max)");
        addColumnType(JDBCType.TIME, "time(6)");
        addColumnType(JDBCType.TIMESTAMP, "datetime2(6)");

    }

}
