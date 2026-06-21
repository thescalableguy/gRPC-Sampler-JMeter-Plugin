package org.apache.jmeter.protocol.grpc.sampler;

import com.google.protobuf.Descriptors;
import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.config.gui.ArgumentsPanel;
import org.apache.jmeter.samplers.gui.AbstractSamplerGui;
import org.apache.jmeter.testelement.TestElement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.swing.*;
import java.awt.*;
import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Swing GUI for configuring the GrpcSampler.
 */
public class GrpcSamplerGui extends AbstractSamplerGui {
    private static final Logger log = LoggerFactory.getLogger(GrpcSamplerGui.class);

    private JTextField hostField;
    private JTextField portField;
    private JCheckBox useTlsCheckBox;
    private JTextField connTimeoutField;
    private JTextField deadlineTimeoutField;
    private JTextField keepAliveField;

    private JCheckBox useReflectionCheckBox;
    private JCheckBox enableOtelCheckBox;

    private JTextField protoPathField;
    private JTextField importPathsField;
    private JButton browseProtoBtn;
    private JButton loadProtoBtn;

    private JComboBox<String> serviceComboBox;
    private JComboBox<String> methodComboBox;

    private ArgumentsPanel metadataPanel;
    private JTextArea payloadArea;
    private JButton generateTemplateBtn;

    private transient List<Descriptors.ServiceDescriptor> resolvedServices = new ArrayList<>();

    public GrpcSamplerGui() {
        init();
    }

    @Override
    public String getStaticLabel() {
        return "gRPC Request";
    }

    @Override
    public String getLabelResource() {
        return null;
    }

    @Override
    public TestElement createTestElement() {
        GrpcSampler sampler = new GrpcSampler();
        modifyTestElement(sampler);
        return sampler;
    }

    @Override
    public void modifyTestElement(TestElement element) {
        configureTestElement(element);
        GrpcSampler sampler = (GrpcSampler) element;
        sampler.setHost(hostField.getText());
        sampler.setPort(portField.getText());
        sampler.setUseTls(useTlsCheckBox.isSelected());
        sampler.setConnectionTimeout(connTimeoutField.getText());
        sampler.setDeadlineTimeout(deadlineTimeoutField.getText());
        sampler.setKeepAlive(keepAliveField.getText());
        
        sampler.setUseReflection(useReflectionCheckBox.isSelected());
        sampler.setEnableOtel(enableOtelCheckBox.isSelected());
        
        sampler.setProtoPath(protoPathField.getText());
        sampler.setImportPaths(importPathsField.getText());
        sampler.setServiceName((String) serviceComboBox.getSelectedItem());
        sampler.setMethodName((String) methodComboBox.getSelectedItem());
        
        sampler.setMetadata((Arguments) metadataPanel.createTestElement());
        sampler.setPayload(payloadArea.getText());
    }

    @Override
    public void configure(TestElement element) {
        super.configure(element);
        GrpcSampler sampler = (GrpcSampler) element;
        hostField.setText(sampler.getHost());
        portField.setText(sampler.getPort());
        useTlsCheckBox.setSelected(sampler.isUseTls());
        connTimeoutField.setText(sampler.getConnectionTimeout());
        deadlineTimeoutField.setText(sampler.getDeadlineTimeout());
        keepAliveField.setText(sampler.getKeepAlive());
        
        useReflectionCheckBox.setSelected(sampler.isUseReflection());
        enableOtelCheckBox.setSelected(sampler.isEnableOtel());
        toggleReflectionState();
        
        protoPathField.setText(sampler.getProtoPath());
        importPathsField.setText(sampler.getImportPaths());
        
        metadataPanel.configure(sampler.getMetadata());
        payloadArea.setText(sampler.getPayload());

        // Attempt to auto-load services if proto path is set or reflection is configured
        if (sampler.isUseReflection()) {
            loadServices(null, null, sampler.getServiceName(), sampler.getMethodName());
        } else if (!sampler.getProtoPath().isEmpty()) {
            loadServices(sampler.getProtoPath(), sampler.getImportPaths(), sampler.getServiceName(), sampler.getMethodName());
        } else {
            clearProtoDropdowns();
        }
    }

    @Override
    public void clearGui() {
        super.clearGui();
        hostField.setText("localhost");
        portField.setText("50051");
        useTlsCheckBox.setSelected(false);
        connTimeoutField.setText("5000");
        deadlineTimeoutField.setText("10000");
        keepAliveField.setText("");
        
        useReflectionCheckBox.setSelected(false);
        enableOtelCheckBox.setSelected(false);
        toggleReflectionState();
        
        protoPathField.setText("");
        importPathsField.setText("");
        clearProtoDropdowns();
        
        metadataPanel.clearGui();
        payloadArea.setText("");
    }

    private void init() {
        setLayout(new BorderLayout(0, 5));
        setBorder(makeBorder());

        // Title panel
        add(makeTitlePanel(), BorderLayout.NORTH);

        // Center scroll pane for configurations
        JPanel centerPanel = new JPanel();
        centerPanel.setLayout(new BoxLayout(centerPanel, BoxLayout.Y_AXIS));

        centerPanel.add(createConnectionPanel());
        centerPanel.add(Box.createVerticalStrut(5));
        centerPanel.add(createProtoPanel());
        centerPanel.add(Box.createVerticalStrut(5));
        centerPanel.add(createMetadataPanel());
        centerPanel.add(Box.createVerticalStrut(5));
        centerPanel.add(createPayloadPanel());

        JScrollPane scrollPane = new JScrollPane(centerPanel);
        scrollPane.setBorder(BorderFactory.createEmptyBorder());
        add(scrollPane, BorderLayout.CENTER);
    }

    private JPanel createConnectionPanel() {
        JPanel panel = new JPanel(new GridBagLayout());
        panel.setBorder(BorderFactory.createTitledBorder("Server Configuration"));
        GridBagConstraints gbc = new GridBagConstraints();
        gbc.fill = GridBagConstraints.HORIZONTAL;
        gbc.insets = new Insets(4, 4, 4, 4);

        // Host
        gbc.gridx = 0; gbc.gridy = 0; gbc.weightx = 0.0;
        panel.add(new JLabel("Target Host:"), gbc);
        gbc.gridx = 1; gbc.weightx = 1.0;
        hostField = new JTextField(15);
        panel.add(hostField, gbc);

        // Port
        gbc.gridx = 2; gbc.weightx = 0.0;
        panel.add(new JLabel("Port:"), gbc);
        gbc.gridx = 3; gbc.weightx = 0.5;
        portField = new JTextField(6);
        panel.add(portField, gbc);

        // TLS checkbox
        gbc.gridx = 4; gbc.weightx = 0.0;
        useTlsCheckBox = new JCheckBox("Use TLS");
        panel.add(useTlsCheckBox, gbc);

        // Row 2: Timeouts
        gbc.gridy = 1;

        // Conn Timeout
        gbc.gridx = 0; gbc.weightx = 0.0;
        panel.add(new JLabel("Connection Timeout (ms):"), gbc);
        gbc.gridx = 1; gbc.weightx = 1.0;
        connTimeoutField = new JTextField(8);
        panel.add(connTimeoutField, gbc);

        // Deadline Timeout
        gbc.gridx = 2; gbc.weightx = 0.0;
        panel.add(new JLabel("Deadline Timeout (ms):"), gbc);
        gbc.gridx = 3; gbc.weightx = 0.5;
        deadlineTimeoutField = new JTextField(8);
        panel.add(deadlineTimeoutField, gbc);

        // Keep Alive
        gbc.gridy = 2;
        gbc.gridx = 0; gbc.weightx = 0.0;
        panel.add(new JLabel("Keep Alive (ms):"), gbc);
        gbc.gridx = 1; gbc.weightx = 1.0;
        keepAliveField = new JTextField(8);
        panel.add(keepAliveField, gbc);

        // OpenTelemetry Checkbox
        gbc.gridy = 3; gbc.gridx = 0; gbc.gridwidth = 5; gbc.weightx = 1.0;
        enableOtelCheckBox = new JCheckBox("Enable OpenTelemetry Trace Context Injection (traceparent)");
        panel.add(enableOtelCheckBox, gbc);

        // Reset gridwidth
        gbc.gridwidth = 1;

        return panel;
    }

    private JPanel createProtoPanel() {
        JPanel panel = new JPanel(new GridBagLayout());
        panel.setBorder(BorderFactory.createTitledBorder("Proto Management"));
        GridBagConstraints gbc = new GridBagConstraints();
        gbc.fill = GridBagConstraints.HORIZONTAL;
        gbc.insets = new Insets(4, 4, 4, 4);

        // Row 0: Server Reflection Checkbox
        gbc.gridx = 0; gbc.gridy = 0; gbc.gridwidth = 3; gbc.weightx = 1.0;
        useReflectionCheckBox = new JCheckBox("Use Server Reflection (Query server dynamically, no .proto upload needed)");
        useReflectionCheckBox.addActionListener(e -> toggleReflectionState());
        panel.add(useReflectionCheckBox, gbc);
        
        gbc.gridwidth = 1; // reset

        // Row 1: Proto File Path
        gbc.gridy = 1; gbc.gridx = 0; gbc.weightx = 0.0;
        panel.add(new JLabel("Proto Path:"), gbc);
        
        gbc.gridx = 1; gbc.weightx = 1.0;
        protoPathField = new JTextField();
        panel.add(protoPathField, gbc);

        gbc.gridx = 2; gbc.weightx = 0.0;
        browseProtoBtn = new JButton("Browse...");
        browseProtoBtn.addActionListener(e -> {
            JFileChooser chooser = new JFileChooser();
            chooser.setFileSelectionMode(JFileChooser.FILES_ONLY);
            int val = chooser.showOpenDialog(this);
            if (val == JFileChooser.APPROVE_OPTION) {
                protoPathField.setText(chooser.getSelectedFile().getAbsolutePath());
            }
        });
        panel.add(browseProtoBtn, gbc);

        // Row 2: Import Paths
        gbc.gridy = 2;
        gbc.gridx = 0; gbc.weightx = 0.0;
        panel.add(new JLabel("Import Paths (comma-separated):"), gbc);

        gbc.gridx = 1; gbc.weightx = 1.0;
        importPathsField = new JTextField();
        panel.add(importPathsField, gbc);

        gbc.gridx = 2; gbc.weightx = 0.0;
        loadProtoBtn = new JButton("Load / Refresh");
        loadProtoBtn.addActionListener(e -> loadServices(protoPathField.getText(), importPathsField.getText(), null, null));
        panel.add(loadProtoBtn, gbc);

        // Row 3: Service & Method dropdowns
        gbc.gridy = 3;
        gbc.gridx = 0; gbc.weightx = 0.0;
        panel.add(new JLabel("Service:"), gbc);

        gbc.gridx = 1; gbc.weightx = 1.0;
        serviceComboBox = new JComboBox<>();
        serviceComboBox.addActionListener(e -> onServiceSelected());
        panel.add(serviceComboBox, gbc);

        gbc.gridy = 4;
        gbc.gridx = 0; gbc.weightx = 0.0;
        panel.add(new JLabel("Method:"), gbc);

        gbc.gridx = 1; gbc.weightx = 1.0;
        methodComboBox = new JComboBox<>();
        methodComboBox.addActionListener(e -> onMethodSelected());
        panel.add(methodComboBox, gbc);

        return panel;
    }

    private void toggleReflectionState() {
        boolean useReflection = useReflectionCheckBox.isSelected();
        protoPathField.setEnabled(!useReflection);
        importPathsField.setEnabled(!useReflection);
        browseProtoBtn.setEnabled(!useReflection);
    }

    private JPanel createMetadataPanel() {
        metadataPanel = new ArgumentsPanel("gRPC Metadata Headers");
        return metadataPanel;
    }

    private JPanel createPayloadPanel() {
        JPanel panel = new JPanel(new BorderLayout(5, 5));
        panel.setBorder(BorderFactory.createTitledBorder("Request Payload (JSON)"));
        
        JPanel buttonPanel = new JPanel(new FlowLayout(FlowLayout.RIGHT));
        generateTemplateBtn = new JButton("Generate JSON Template");
        generateTemplateBtn.addActionListener(e -> generatePayloadTemplate());
        buttonPanel.add(generateTemplateBtn);
        panel.add(buttonPanel, BorderLayout.NORTH);

        payloadArea = new JTextArea(12, 40);
        payloadArea.setFont(new Font(Font.MONOSPACED, Font.PLAIN, 12));
        JScrollPane scroll = new JScrollPane(payloadArea);
        panel.add(scroll, BorderLayout.CENTER);

        return panel;
    }

    // --- Action Handlers ---

    private void loadServices(String protoPath, String importPaths, String selectService, String selectMethod) {
        boolean useReflection = useReflectionCheckBox.isSelected();
        
        if (useReflection) {
            String host = hostField.getText().trim();
            String portStr = portField.getText().trim();
            if (host.isEmpty() || portStr.isEmpty()) {
                // Ignore silent configuration loadings
                if (selectService != null) return;
                JOptionPane.showMessageDialog(this,
                        "Please configure Host and Port to list services via Reflection.",
                        "Configuration Required",
                        JOptionPane.WARNING_MESSAGE);
                return;
            }
            int port;
            try {
                port = Integer.parseInt(portStr);
            } catch (NumberFormatException e) {
                if (selectService != null) return;
                JOptionPane.showMessageDialog(this, "Port must be a valid number.", "Error", JOptionPane.ERROR_MESSAGE);
                return;
            }
            long connTimeout = 5000;
            try {
                connTimeout = Long.parseLong(connTimeoutField.getText().trim());
            } catch (NumberFormatException ignored) {}
            
            try {
                // Fetch dynamic reflections from the channel
                resolvedServices = ReflectionResolver.resolveServices(host, port, useTlsCheckBox.isSelected(), connTimeout, 0, true);
                
                serviceComboBox.removeAllItems();
                for (Descriptors.ServiceDescriptor service : resolvedServices) {
                    serviceComboBox.addItem(service.getFullName());
                }

                if (selectService != null) {
                    serviceComboBox.setSelectedItem(selectService);
                }

                onServiceSelected();

                if (selectMethod != null) {
                    methodComboBox.setSelectedItem(selectMethod);
                }
            } catch (Exception e) {
                log.error("Failed to load services via reflection", e);
                if (selectService == null) {
                    JOptionPane.showMessageDialog(this,
                            "Failed to resolve server reflection: " + e.getMessage(),
                            "Server Reflection Error",
                            JOptionPane.ERROR_MESSAGE);
                }
                clearProtoDropdowns();
            }
        } else {
            // File resolver logic
            if (protoPath == null || protoPath.trim().isEmpty()) {
                return;
            }

            List<String> importPathsList = new ArrayList<>();
            if (importPaths != null && !importPaths.isEmpty()) {
                for (String path : importPaths.split(",")) {
                    importPathsList.add(path.trim());
                }
            }

            try {
                resolvedServices = ProtoResolver.resolveServices(protoPath.trim(), importPathsList);
                
                serviceComboBox.removeAllItems();
                for (Descriptors.ServiceDescriptor service : resolvedServices) {
                    serviceComboBox.addItem(service.getFullName());
                }

                if (selectService != null) {
                    serviceComboBox.setSelectedItem(selectService);
                }

                onServiceSelected();

                if (selectMethod != null) {
                    methodComboBox.setSelectedItem(selectMethod);
                }

            } catch (Exception e) {
                log.error("Failed to load services from proto file", e);
                if (selectService == null) {
                    JOptionPane.showMessageDialog(this,
                            "Failed to parse proto file: " + e.getMessage(),
                            "Proto Parsing Error",
                            JOptionPane.ERROR_MESSAGE);
                }
                clearProtoDropdowns();
            }
        }
    }

    private void onServiceSelected() {
        String selectedService = (String) serviceComboBox.getSelectedItem();
        methodComboBox.removeAllItems();
        if (selectedService == null) return;

        Descriptors.ServiceDescriptor matchedService = null;
        for (Descriptors.ServiceDescriptor service : resolvedServices) {
            if (service.getFullName().equals(selectedService)) {
                matchedService = service;
                break;
            }
        }

        if (matchedService != null) {
            for (Descriptors.MethodDescriptor method : matchedService.getMethods()) {
                methodComboBox.addItem(method.getName());
            }
        }
    }

    private void onMethodSelected() {
        // Auto-fill template if payload is completely empty
        if (payloadArea.getText().trim().isEmpty()) {
            generatePayloadTemplate();
        }
    }

    private void generatePayloadTemplate() {
        String selectedService = (String) serviceComboBox.getSelectedItem();
        String selectedMethod = (String) methodComboBox.getSelectedItem();
        if (selectedService == null || selectedMethod == null) {
            if (payloadArea.isFocusOwner()) {
                JOptionPane.showMessageDialog(this,
                        "Please select a Service and Method first.",
                        "Validation Error",
                        JOptionPane.WARNING_MESSAGE);
            }
            return;
        }

        Descriptors.MethodDescriptor matchedMethod = null;
        for (Descriptors.ServiceDescriptor service : resolvedServices) {
            if (service.getFullName().equals(selectedService)) {
                matchedMethod = service.findMethodByName(selectedMethod);
                break;
            }
        }

        if (matchedMethod != null) {
            String template = JsonTemplateGenerator.generateTemplate(matchedMethod.getInputType());
            payloadArea.setText(template);
        }
    }

    private void clearProtoDropdowns() {
        resolvedServices = new ArrayList<>();
        serviceComboBox.removeAllItems();
        methodComboBox.removeAllItems();
    }
}
