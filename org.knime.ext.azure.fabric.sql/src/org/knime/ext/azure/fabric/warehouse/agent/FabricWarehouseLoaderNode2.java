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
package org.knime.ext.azure.fabric.warehouse.agent;

import static java.util.Arrays.asList;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.JDBCType;
import java.sql.SQLType;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.List;
import java.util.Optional;

import javax.swing.JPanel;

import org.apache.parquet.hadoop.ParquetFileWriter.Mode;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.eclipse.core.runtime.URIUtil;
import org.knime.bigdata.database.loader.BigDataLoaderParameters2;
import org.knime.bigdata.fileformats.parquet.ParquetFileFormatWriter;
import org.knime.bigdata.fileformats.parquet.datatype.mapping.ParquetType;
import org.knime.bigdata.fileformats.parquet.datatype.mapping.ParquetTypeMappingService;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.convert.map.ConsumptionPath;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.defaultnodesettings.DialogComponent;
import org.knime.core.node.defaultnodesettings.SettingsModel;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.streamable.RowInput;
import org.knime.database.DBTableSpec;
import org.knime.database.agent.loader.DBLoadTableFromFileParameters;
import org.knime.database.agent.loader.DBLoader;
import org.knime.database.agent.loader.DBLoaderMode;
import org.knime.database.connection.DBConnectionManagerAttributes;
import org.knime.database.model.DBColumn;
import org.knime.database.model.DBTable;
import org.knime.database.node.io.load.DBLoaderNode2;
import org.knime.database.node.io.load.DBLoaderNode2Factory;
import org.knime.database.node.io.load.DBLoaderParameters;
import org.knime.database.node.io.load.ExecutionParameters;
import org.knime.database.node.io.load.impl.fs.ConnectableCsvLoaderNodeSettings2;
import org.knime.database.node.io.load.impl.fs.ConnectedCsvLoaderNodeComponents2;
import org.knime.database.node.io.load.impl.fs.ConnectedLoaderNode2;
import org.knime.database.node.io.load.impl.fs.DBFileLoadUtil;
import org.knime.database.node.io.load.impl.fs.DBFileLoadUtil.DBFileLoader;
import org.knime.database.port.DBDataPortObjectSpec;
import org.knime.database.port.DBPortObject;
import org.knime.database.session.DBSession;
import org.knime.datatype.mapping.DataTypeMappingConfiguration;
import org.knime.datatype.mapping.DataTypeMappingDirection;
import org.knime.filehandling.core.connections.FSConnection;
import org.knime.filehandling.core.connections.FSPath;
import org.knime.filehandling.core.connections.uriexport.URIExporter;
import org.knime.filehandling.core.connections.uriexport.URIExporterIDs;
import org.knime.filehandling.core.connections.uriexport.noconfig.NoConfigURIExporterFactory;
import org.knime.filehandling.core.defaultnodesettings.filechooser.writer.SettingsModelWriterFileChooser;

/**
 * Class for Big Data Loader node
 *
 * @author Tobias Koetter, KNIME GmbH, Konstanz, Germany
 */
public class FabricWarehouseLoaderNode2
    extends ConnectedLoaderNode2<ConnectedCsvLoaderNodeComponents2, ConnectableCsvLoaderNodeSettings2>
    implements DBLoaderNode2Factory<ConnectedCsvLoaderNodeComponents2, ConnectableCsvLoaderNodeSettings2> {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(FabricWarehouseLoaderNode2.class);

    private static final int TO_BYTE = 1024 * 1024;

    private static final long FILE_SIZE = TO_BYTE * 1024l;

    private static final int ROW_GROUP_SIZE = ParquetWriter.DEFAULT_BLOCK_SIZE * TO_BYTE;

    private static final boolean USE_LOGICAL_TYPES = false;


    private static final class DBLoaderFileWriterImplementation
        implements DBFileLoader<ConnectableCsvLoaderNodeSettings2, BigDataLoaderParameters2> {

        private static final EnumMap<JDBCType, ParquetType> m_JDBCToParquetMap = createParquetMap();

        private static EnumMap<JDBCType, ParquetType> createParquetMap() {
            EnumMap<JDBCType, ParquetType> typeMap = new EnumMap<>(JDBCType.class);

            typeMap.put(JDBCType.BIGINT, new ParquetType(PrimitiveTypeName.INT64));
            typeMap.put(JDBCType.BINARY, new ParquetType(PrimitiveTypeName.BINARY));
            typeMap.put(JDBCType.BIT, new ParquetType(PrimitiveTypeName.BOOLEAN));
            typeMap.put(JDBCType.INTEGER, new ParquetType(PrimitiveTypeName.INT32));
            typeMap.put(JDBCType.DATE, new ParquetType(PrimitiveTypeName.INT32, OriginalType.DATE));
            typeMap.put(JDBCType.DOUBLE, new ParquetType(PrimitiveTypeName.DOUBLE));
            typeMap.put(JDBCType.TIME, new ParquetType(PrimitiveTypeName.INT32, OriginalType.TIME_MILLIS));
            typeMap.put(JDBCType.TIMESTAMP, new ParquetType(PrimitiveTypeName.INT64, OriginalType.TIMESTAMP_MICROS));
            typeMap.put(JDBCType.VARCHAR, new ParquetType(PrimitiveTypeName.BINARY, OriginalType.UTF8));


            return typeMap;
        }

        @Override
        public String getFileExtension() {
            return ".parquet";
        }

        @Override
        public BigDataLoaderParameters2 getLoadParameter(final ConnectableCsvLoaderNodeSettings2 settings) {
            return null;
        }

        @Override
        public void writerAndLoad(final ExecutionParameters<ConnectableCsvLoaderNodeSettings2> parameters,
                final ExecutionMonitor exec, final FSPath targetFile, final DBLoaderMode mode, final DBSession session,
                final DBTable table) throws Exception {
            exec.setMessage("Writing parquet file...");
            final RowInput rowInput = parameters.getRowInput();
            final DataTableSpec spec = rowInput.getDataTableSpec();
            final ConnectableCsvLoaderNodeSettings2 customSettings = parameters.getCustomSettings();
            final SettingsModelWriterFileChooser targetFolderModel = customSettings.getTargetFolderModel();
            try (FSConnection connection = targetFolderModel.getConnection()) {
                final NoConfigURIExporterFactory uriExporterFactory = (NoConfigURIExporterFactory) connection
                        .getURIExporterFactory(URIExporterIDs.DEFAULT);
                if (uriExporterFactory == null) {
                    LOGGER.debug(String.format("Connected file system '%s' does not provide a URI exporter",
                            connection.getFSType().getName()));
                    throw new InvalidSettingsException("Connected file system is not supported");
                }
                final var dbTableSpec = getDBTableSpecification(exec, table, session);
                try (ParquetFileFormatWriter writer = createWriter(targetFile, spec, dbTableSpec)) {
                    DataRow row;
                    while ((row = rowInput.poll()) != null) {
                        writer.writeRow(row);
                    }
                    LOGGER.debug("Written file " + targetFile + " ");
                    exec.setProgress(0.25, "File written.");
                    exec.checkCanceled();
                } finally {
                    rowInput.close();
                }
                final URIExporter uriExporter = uriExporterFactory.getExporter();
                final String targetFileString = URIUtil.toUnencodedString(uriExporter.toUri(targetFile));
                exec.setProgress("Loading data file into DB table...");
                exec.checkCanceled();
                session.getAgent(DBLoader.class).load(exec, new DBLoadTableFromFileParameters<>(mode, targetFileString,
                        table, getLoadParameter(customSettings)));
            }
        }

        @Override
        public void writerMethod(final ExecutionParameters<ConnectableCsvLoaderNodeSettings2> parameters,
            final ExecutionMonitor executionContext, final OutputStream outputStream)
            throws CanceledExecutionException, IOException {
            throw new IllegalStateException("Shouldn't be called");
        }

        /**
         * Creates a {@link ParquetFileFormatWriter}
         *
         * @param file
         *            the file to write to
         * @param knimeSpec
         *            the DataTableSpec of the input
         * @param dbTableSpec
         * @param dbTableSpec
         *            List of columns in order of input table, renamed to generic names
         * @return ParquetFileFormatWriter to use
         * @throws IOException
         *             if writer cannot be initialized
         */
        private static ParquetFileFormatWriter createWriter(final FSPath file, final DataTableSpec knimeSpec,
                final DBTableSpec dbTableSpec) throws IOException {
            final CompressionCodecName compression = CompressionCodecName.SNAPPY;
            return new ParquetFileFormatWriter(file, Mode.OVERWRITE, knimeSpec, compression, FILE_SIZE, ROW_GROUP_SIZE,
                    getParquetTypesMapping(knimeSpec, dbTableSpec.getColumns()), USE_LOGICAL_TYPES);
        }

        private static DataTypeMappingConfiguration<ParquetType> getParquetTypesMapping(final DataTableSpec spec,
                final DBColumn[] dbColumns) {

            final List<ParquetType> parquetTypes = mapDBToParquetTypes(dbColumns);

            final DataTypeMappingConfiguration<ParquetType> configuration = ParquetTypeMappingService.getInstance()
                .createMappingConfiguration(DataTypeMappingDirection.KNIME_TO_EXTERNAL);

            for (int i = 0; i < spec.getNumColumns(); i++) {
                final DataColumnSpec knimeCol = spec.getColumnSpec(i);
                final DataType dataType = knimeCol.getType();
                final ParquetType parquetType = parquetTypes.get(i);
                final Collection<ConsumptionPath> consumPaths =
                    ParquetTypeMappingService.getInstance().getConsumptionPathsFor(dataType);

                final Optional<ConsumptionPath> path = consumPaths.stream()
                    .filter(p -> p.getConsumerFactory().getDestinationType().equals(parquetType)).findFirst();
                if (path.isPresent()) {
                    configuration.addRule(dataType, path.get());
                } else {
                    final String error =
                        String.format("Could not find ConsumptionPath for %s to JDBC Type %s via Parquet Type %s", dataType,
                                    dbColumns[i].getColumnTypeName(), parquetType);
                    LOGGER.error(error);
                    throw new IllegalStateException(error);
                }
            }

            return configuration;
        }

        private static List<ParquetType> mapDBToParquetTypes(final DBColumn[] inputColumns) {
            final List<ParquetType> parquetTypes = new ArrayList<>();
            for (final DBColumn dbCol : inputColumns) {
                final SQLType type = dbCol.getColumnType();
                final ParquetType parquetType = m_JDBCToParquetMap.get(type);
                if (parquetType == null) {
                    throw new IllegalStateException(String.format("Cannot find Parquet type for Database type %s", type));
                }
                parquetTypes.add(parquetType);
            }
            return parquetTypes;
        }
    }

    @Override
    public DBLoaderNode2<ConnectedCsvLoaderNodeComponents2, ConnectableCsvLoaderNodeSettings2> get() {
        return new FabricWarehouseLoaderNode2();
    }

    @Override
    public void buildDialog(final DialogBuilder builder, final List<DialogComponent> dialogComponents,
        final ConnectedCsvLoaderNodeComponents2 customComponents) {
        final JPanel optionsPanel = createTargetTableFolderPanel(customComponents);
        builder.addTab(Integer.MAX_VALUE, "Options", optionsPanel, true);
    }

    @Override
    public DBDataPortObjectSpec configureModel(final PortObjectSpec[] inSpecs,
        final List<SettingsModel> settingsModels, final ConnectableCsvLoaderNodeSettings2 customSettings)
        throws InvalidSettingsException {

        if (inSpecs.length != 3) {
            throw new InvalidSettingsException("File system connection missing. Microsoft Fabric supports only BLOB "
                    + "and ADLS Gen2 storage accounts to copy data from. For more details see the documentation "
                    + "(https://learn.microsoft.com/en-us/sql/t-sql/statements/copy-into-transact-sql?view=fabric).");
        }

        // TK_TODO: Check if it is ADLS Gen2 or BLOB storage
        // final FSLocationSpec fsLocationSpec = ((FileSystemPortObjectSpec)
        // inSpecs[0]).getFSLocationSpec();
        // Optional<String> fileSystemSpecifier =
        // fsLocationSpec.getFileSystemSpecifier();
        // ADLSFSDescProvider.validateFileSystemSpecifier(fileSystemSpecifier);

        final DBPortObject sessionPortObjectSpec = getDBSpec(inSpecs);
        final DBSession session = sessionPortObjectSpec.getDBSession();

        if (session.getAttributeValues().get(DBConnectionManagerAttributes.ATTRIBUTE_METADATA_IN_CONFIGURE_ENABLED)) {

            final ExecutionMonitor exec = createModelConfigurationExecutionMonitor(session);
            final DataTableSpec knimeTableSpec = getDataSpec(inSpecs);
            final DBTable dbTable = customSettings.getTableNameModel().toDBTable();

            try {
                validateColumns(false, exec, knimeTableSpec, sessionPortObjectSpec, dbTable);
            } catch (final InvalidSettingsException e) {
                throw e;
            } catch (final Exception ex) {
                throw new InvalidSettingsException(ex);
            }
        }

        return super.configureModel(inSpecs, settingsModels, customSettings);
    }

    @Override
    public ConnectedCsvLoaderNodeComponents2 createCustomDialogComponents(final DialogDelegate dialogDelegate) {
        return new ConnectedCsvLoaderNodeComponents2(dialogDelegate);
    }

    @Override
    public ConnectableCsvLoaderNodeSettings2 createCustomModelSettings(final ModelDelegate modelDelegate) {
        return new ConnectableCsvLoaderNodeSettings2(modelDelegate);
    }

    @Override
    public List<DialogComponent> createDialogComponents(final ConnectedCsvLoaderNodeComponents2 customComponents) {
        return asList(customComponents.getTargetFolderComponent(), customComponents.getTableNameComponent());
    }

    @Override
    public void onCloseInDialog(final ConnectedCsvLoaderNodeComponents2 customComponents) {
        super.onCloseInDialog(customComponents);
        customComponents.getTargetFolderComponent().onClose();
    }

    @Override
    public List<SettingsModel> createSettingsModels(final ConnectableCsvLoaderNodeSettings2 customSettings) {
        return asList(customSettings.getTargetFolderModel(), customSettings.getTableNameModel());
    }

    @Override
    public DBTable load(final ExecutionParameters<ConnectableCsvLoaderNodeSettings2> parameters)
        throws Exception {
        final DBPortObject sessionPortObject = parameters.getDBPortObject();
        final DBSession session = sessionPortObject.getDBSession();
        final ExecutionMonitor exec = parameters.getExecutionMonitor();
        final ConnectableCsvLoaderNodeSettings2 customSettings = parameters.getCustomSettings();
        final DBTable dbTable = customSettings.getTableNameModel().toDBTable();
        final DataTableSpec knimeTableSpec = parameters.getRowInput().getDataTableSpec();

        validateColumns(false, exec, knimeTableSpec, sessionPortObject, dbTable);

        //Write the file
        DBFileLoadUtil.writeAndLoadFile(exec, parameters, null, session, dbTable,
                new DBLoaderFileWriterImplementation());

        // Output
        return dbTable;
    }

    @Override
    public Class<? extends DBLoaderParameters> getParametersClass() {
        return FabricWarehouseLoaderNodeParameters.class;
    }

}
