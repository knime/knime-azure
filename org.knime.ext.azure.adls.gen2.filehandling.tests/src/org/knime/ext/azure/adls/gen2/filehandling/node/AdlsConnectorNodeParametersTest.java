package org.knime.ext.azure.adls.gen2.filehandling.node;

import java.io.FileInputStream;
import java.io.IOException;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettings;
import org.knime.core.webui.node.dialog.SettingsType;
import org.knime.core.webui.node.dialog.defaultdialog.NodeParametersUtil;
import org.knime.testing.node.dialog.DefaultNodeSettingsSnapshotTest;
import org.knime.testing.node.dialog.SnapshotTestConfiguration;

@SuppressWarnings("restriction")
final class AdlsConnectorNodeParametersTest extends DefaultNodeSettingsSnapshotTest {

    AdlsConnectorNodeParametersTest() {
        super(getConfig());
    }

    private static SnapshotTestConfiguration getConfig() {
        return SnapshotTestConfiguration.builder() //
                .testJsonFormsForModel(AdlsConnectorNodeParameters.class) //
                .testJsonFormsWithInstance(SettingsType.MODEL, () -> readSettings()) //
                .testNodeSettingsStructure(() -> readSettings()) //
                .build();
    }

    private static AdlsConnectorNodeParameters readSettings() {
        try {
            var path = getSnapshotPath(AdlsConnectorNodeParametersTest.class).getParent() //
                    .resolve("node_settings").resolve("AdlsConnectorNodeParameters.xml");
            try (var fis = new FileInputStream(path.toFile())) {
                var nodeSettings = NodeSettings.loadFromXML(fis);
                return NodeParametersUtil.loadSettings(
                        nodeSettings.getNodeSettings(SettingsType.MODEL.getConfigKey()),
                        AdlsConnectorNodeParameters.class);
            }
        } catch (IOException | InvalidSettingsException e) {
            throw new IllegalStateException(e);
        }
    }
}
