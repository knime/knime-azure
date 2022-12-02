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
 *   2020-07-16 (Alexander Bondaletov): created
 */
package org.knime.ext.azure.blobstorage.filehandling.testing;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;

import org.knime.core.node.util.CheckUtils;
import org.knime.ext.azure.blobstorage.filehandling.fs.AzureBlobStorageFSConnection;
import org.knime.ext.azure.blobstorage.filehandling.fs.AzureBlobStorageFSConnectionConfig;
import org.knime.ext.azure.blobstorage.filehandling.fs.AzureBlobStorageFSDescriptorProvider;
import org.knime.ext.azure.blobstorage.filehandling.fs.AzureBlobStorageFileSystem;
import org.knime.ext.microsoft.authentication.port.azure.storage.AzureSharedKeyCredential;
import org.knime.filehandling.core.connections.FSLocationSpec;
import org.knime.filehandling.core.connections.meta.FSType;
import org.knime.filehandling.core.connections.meta.base.BaseFSConnectionConfig.BrowserRelativizationBehavior;
import org.knime.filehandling.core.testing.DefaultFSTestInitializerProvider;

/**
 * Initializer provider for ths Azure Blob Storage.
 *
 * @author Alexander Bondaletov
 */
public class AzureBlobStorageTestInitializerProvider extends DefaultFSTestInitializerProvider {

    @SuppressWarnings("resource")
    @Override
    public AzureBlobStorageTestInitializer setup(final Map<String, String> configuration) throws IOException {
        validateConfiguration(configuration);

        final String workingDir = generateRandomizedWorkingDir(configuration.get("workingDirPrefix"),
                AzureBlobStorageFileSystem.PATH_SEPARATOR);

        AzureBlobStorageFSConnectionConfig config = new AzureBlobStorageFSConnectionConfig(workingDir,
                BrowserRelativizationBehavior.ABSOLUTE);
        config.setCredential(new AzureSharedKeyCredential(getParameter(configuration, "account"), null) {
            @Override
            public String getSecretKey() {
                return getParameter(configuration, "key");
            }
        });
        config.setNormalizePaths(true);
        config.setTimeout(Duration.ofSeconds(AzureBlobStorageFSConnectionConfig.DEFAULT_TIMEOUT));

        return new AzureBlobStorageTestInitializer(new AzureBlobStorageFSConnection(config));
    }

    private static void validateConfiguration(final Map<String, String> configuration) {
        CheckUtils.checkArgumentNotNull(configuration.get("account"), "account must be specified.");
        CheckUtils.checkArgumentNotNull(configuration.get("key"), "key must be specified.");
        CheckUtils.checkArgumentNotNull(configuration.get("workingDirPrefix"), "workingDirPrefix must be specified.");
    }

    @Override
    public FSType getFSType() {
        return AzureBlobStorageFSDescriptorProvider.FS_TYPE;
    }

    @Override
    public FSLocationSpec createFSLocationSpec(final Map<String, String> configuration) {
        validateConfiguration(configuration);
        return AzureBlobStorageFSConnectionConfig.createFSLocationSpec(configuration.get("account"));
    }

}
