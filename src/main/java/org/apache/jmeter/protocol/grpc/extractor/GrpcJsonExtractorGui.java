package org.apache.jmeter.protocol.grpc.extractor;

import org.apache.jmeter.processor.gui.AbstractPostProcessorGui;
import org.apache.jmeter.testelement.TestElement;

import javax.swing.*;
import java.awt.*;

/**
 * Swing GUI for configuring the GrpcJsonExtractor.
 */
public class GrpcJsonExtractorGui extends AbstractPostProcessorGui {

    private JTextField varNamesField;
    private JTextField jsonPathExprsField;
    private JTextField matchNumbersField;
    private JTextField defaultValuesField;

    public GrpcJsonExtractorGui() {
        init();
    }

    @Override
    public String getStaticLabel() {
        return "gRPC JSON Extractor";
    }

    @Override
    public String getLabelResource() {
        return null;
    }

    @Override
    public TestElement createTestElement() {
        GrpcJsonExtractor extractor = new GrpcJsonExtractor();
        modifyTestElement(extractor);
        return extractor;
    }

    @Override
    public void modifyTestElement(TestElement element) {
        configureTestElement(element);
        GrpcJsonExtractor extractor = (GrpcJsonExtractor) element;
        extractor.setVarNames(varNamesField.getText());
        extractor.setJsonPathExprs(jsonPathExprsField.getText());
        extractor.setMatchNumbers(matchNumbersField.getText());
        extractor.setDefaultValues(defaultValuesField.getText());
    }

    @Override
    public void configure(TestElement element) {
        super.configure(element);
        GrpcJsonExtractor extractor = (GrpcJsonExtractor) element;
        varNamesField.setText(extractor.getVarNames());
        jsonPathExprsField.setText(extractor.getJsonPathExprs());
        matchNumbersField.setText(extractor.getMatchNumbers());
        defaultValuesField.setText(extractor.getDefaultValues());
    }

    @Override
    public void clearGui() {
        super.clearGui();
        varNamesField.setText("");
        jsonPathExprsField.setText("");
        matchNumbersField.setText("1");
        defaultValuesField.setText("");
    }

    private void init() {
        setLayout(new BorderLayout(0, 5));
        setBorder(makeBorder());

        add(makeTitlePanel(), BorderLayout.NORTH);

        JPanel mainPanel = new JPanel(new GridBagLayout());
        mainPanel.setBorder(BorderFactory.createTitledBorder("Extractor Settings"));
        
        GridBagConstraints gbc = new GridBagConstraints();
        gbc.fill = GridBagConstraints.HORIZONTAL;
        gbc.insets = new Insets(4, 4, 4, 4);

        // Created Variable Names
        gbc.gridx = 0; gbc.gridy = 0; gbc.weightx = 0.0;
        mainPanel.add(new JLabel("Names of created variables (comma-separated):"), gbc);
        gbc.gridx = 1; gbc.weightx = 1.0;
        varNamesField = new JTextField();
        mainPanel.add(varNamesField, gbc);

        // JSONPath Expressions
        gbc.gridx = 0; gbc.gridy = 1; gbc.weightx = 0.0;
        mainPanel.add(new JLabel("JSONPath expressions (comma-separated):"), gbc);
        gbc.gridx = 1; gbc.weightx = 1.0;
        jsonPathExprsField = new JTextField();
        mainPanel.add(jsonPathExprsField, gbc);

        // Match Numbers
        gbc.gridx = 0; gbc.gridy = 2; gbc.weightx = 0.0;
        mainPanel.add(new JLabel("Match No. (0 for Random, -1 for All, 1 for First):"), gbc);
        gbc.gridx = 1; gbc.weightx = 1.0;
        matchNumbersField = new JTextField();
        mainPanel.add(matchNumbersField, gbc);

        // Default Values
        gbc.gridx = 0; gbc.gridy = 3; gbc.weightx = 0.0;
        mainPanel.add(new JLabel("Default Values (comma-separated, fallback if no match):"), gbc);
        gbc.gridx = 1; gbc.weightx = 1.0;
        defaultValuesField = new JTextField();
        mainPanel.add(defaultValuesField, gbc);

        add(mainPanel, BorderLayout.CENTER);
    }
}
