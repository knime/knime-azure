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
 *   2020-12-18 (Alexander Bondaletov): created
 */
package org.knime.ext.azure.adls.gen2.filehandling.fs;

import java.util.Map;

import org.knime.core.node.util.FileSystemBrowser;
import org.knime.filehandling.core.connections.FSConnection;
import org.knime.filehandling.core.connections.FSFileSystem;
import org.knime.filehandling.core.connections.uriexport.URIExporter;
import org.knime.filehandling.core.connections.uriexport.URIExporterID;
import org.knime.filehandling.core.connections.uriexport.URIExporterIDs;
import org.knime.filehandling.core.connections.uriexport.URIExporterMapBuilder;
import org.knime.filehandling.core.filechooser.NioFileSystemBrowser;

import com.azure.storage.file.datalake.DataLakeServiceClient;

/**
 * ADLS implementation of the {@link FSConnection} interface.
 *
 * @author Alexander Bondaletov
 */
public class AdlsFSConnection implements FSConnection {
    private static final Map<URIExporterID, URIExporter> URI_EXPORTERS = new URIExporterMapBuilder() //
            .add(URIExporterIDs.DEFAULT, AbfsURIExporter.getInstance()) //
            .add(URIExporterIDs.DEFAULT_HADOOP, AbfsURIExporter.getInstance()) //
            .build();

    private static final long CACHE_TTL = 6000;

    private final AdlsFileSystem m_filesystem;

    /**
     * @param client
     *            The {@link DataLakeServiceClient} instance.
     * @param workingDir
     *            The working directory.
     *
     */
    public AdlsFSConnection(final DataLakeServiceClient client, final String workingDir) {
        m_filesystem = new AdlsFileSystem(client, CACHE_TTL, workingDir);
    }

    @Override
    public FSFileSystem<?> getFileSystem() {
        return m_filesystem;
    }

    @Override
    public FileSystemBrowser getFileSystemBrowser() {
        return new NioFileSystemBrowser(this);
    }

    @Override
    public Map<URIExporterID, URIExporter> getURIExporters() {
        return URI_EXPORTERS;
    }
}