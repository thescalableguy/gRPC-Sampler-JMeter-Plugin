package org.apache.jmeter.protocol.grpc.sampler;

import com.google.protobuf.Descriptors;

import java.util.*;

/**
 * Generates JSON templates for gRPC requests dynamically based on Protobuf descriptors.
 */
public class JsonTemplateGenerator {

    /**
     * Generates a pretty-printed JSON template representing the structure of a given request descriptor.
     *
     * @param descriptor The Protobuf Message Descriptor to generate a template for.
     * @return Pretty-printed JSON template string.
     */
    public static String generateTemplate(Descriptors.Descriptor descriptor) {
        if (descriptor == null) {
            return "{}";
        }
        Object structure = generateFieldStructure(descriptor, new HashSet<>());
        return prettyPrint(structure, 0);
    }

    private static Object generateFieldStructure(Descriptors.Descriptor descriptor, Set<Descriptors.Descriptor> visited) {
        if (visited.contains(descriptor)) {
            return new LinkedHashMap<String, Object>(); // Prevent infinite recursion on recursive message types
        }
        visited.add(descriptor);
        try {
            LinkedHashMap<String, Object> map = new LinkedHashMap<>();
            for (Descriptors.FieldDescriptor field : descriptor.getFields()) {
                String fieldName = field.getJsonName();
                if (field.isRepeated()) {
                    if (field.isMapField()) {
                        // Map type: represented in protobuf as a repeated field containing key/value entries
                        Descriptors.Descriptor entryDesc = field.getMessageType();
                        Descriptors.FieldDescriptor keyField = entryDesc.findFieldByName("key");
                        Descriptors.FieldDescriptor valueField = entryDesc.findFieldByName("value");

                        LinkedHashMap<String, Object> mapTemplate = new LinkedHashMap<>();
                        String keySample = "key";
                        Object valueSample = generateFieldValue(valueField, new HashSet<>(visited));
                        mapTemplate.put(keySample, valueSample);
                        map.put(fieldName, mapTemplate);
                    } else {
                        // List/Array type
                        List<Object> list = new ArrayList<>();
                        list.add(generateFieldValue(field, new HashSet<>(visited)));
                        map.put(fieldName, list);
                    }
                } else {
                    map.put(fieldName, generateFieldValue(field, new HashSet<>(visited)));
                }
            }
            return map;
        } finally {
            visited.remove(descriptor);
        }
    }

    private static Object generateFieldValue(Descriptors.FieldDescriptor field, Set<Descriptors.Descriptor> visited) {
        if (field.getType() == Descriptors.FieldDescriptor.Type.MESSAGE) {
            return generateFieldStructure(field.getMessageType(), visited);
        }
        return getSampleValue(field);
    }

    private static Object getSampleValue(Descriptors.FieldDescriptor field) {
        switch (field.getType()) {
            case INT32:
            case SINT32:
            case SFIXED32:
            case UINT32:
            case FIXED32:
                return 0;
            case INT64:
            case SINT64:
            case SFIXED64:
            case UINT64:
            case FIXED64:
                return 0L;
            case FLOAT:
                return 0.0f;
            case DOUBLE:
                return 0.0d;
            case BOOL:
                return false;
            case STRING:
                return "string";
            case BYTES:
                return ""; // Base64 empty representation
            case ENUM:
                List<Descriptors.EnumValueDescriptor> values = field.getEnumType().getValues();
                return values.isEmpty() ? "UNKNOWN" : values.get(0).getName();
            default:
                return null;
        }
    }

    private static String prettyPrint(Object obj, int indent) {
        StringBuilder sb = new StringBuilder();
        String indentStr = "  ".repeat(indent);
        String childIndentStr = "  ".repeat(indent + 1);

        if (obj instanceof Map) {
            Map<?, ?> map = (Map<?, ?>) obj;
            if (map.isEmpty()) {
                return "{}";
            }
            sb.append("{\n");
            int count = 0;
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                sb.append(childIndentStr)
                        .append("\"").append(entry.getKey()).append("\": ")
                        .append(prettyPrint(entry.getValue(), indent + 1));
                if (++count < map.size()) {
                    sb.append(",");
                }
                sb.append("\n");
            }
            sb.append(indentStr).append("}");
        } else if (obj instanceof List) {
            List<?> list = (List<?>) obj;
            if (list.isEmpty()) {
                return "[]";
            }
            sb.append("[\n");
            int count = 0;
            for (Object item : list) {
                sb.append(childIndentStr)
                        .append(prettyPrint(item, indent + 1));
                if (++count < list.size()) {
                    sb.append(",");
                }
                sb.append("\n");
            }
            sb.append(indentStr).append("]");
        } else if (obj instanceof String) {
            sb.append("\"").append(escapeJsonString((String) obj)).append("\"");
        } else if (obj == null) {
            sb.append("null");
        } else {
            sb.append(obj.toString());
        }
        return sb.toString();
    }

    private static String escapeJsonString(String str) {
        return str.replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t");
    }
}
