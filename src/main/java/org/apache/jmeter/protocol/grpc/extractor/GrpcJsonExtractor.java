package org.apache.jmeter.protocol.grpc.extractor;

import com.jayway.jsonpath.JsonPath;
import org.apache.jmeter.processor.PostProcessor;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.testelement.AbstractTestElement;
import org.apache.jmeter.threads.JMeterContext;
import org.apache.jmeter.threads.JMeterVariables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;
import java.util.Random;

/**
 * JMeter PostProcessor to extract variables from JSON response data of a gRPC Sampler.
 */
public class GrpcJsonExtractor extends AbstractTestElement implements PostProcessor, Serializable {
    private static final Logger log = LoggerFactory.getLogger(GrpcJsonExtractor.class);

    public static final String VAR_NAMES = "GrpcJsonExtractor.varNames";
    public static final String JSON_PATH_EXPRS = "GrpcJsonExtractor.jsonPathExprs";
    public static final String MATCH_NUMBERS = "GrpcJsonExtractor.matchNumbers";
    public static final String DEFAULT_VALUES = "GrpcJsonExtractor.defaultValues";

    private final Random random = new Random();

    @Override
    public void process() {
        JMeterContext context = getThreadContext();
        SampleResult previousResult = context.getPreviousResult();
        if (previousResult == null) {
            return;
        }

        String responseJson = previousResult.getResponseDataAsString();
        if (responseJson == null || responseJson.trim().isEmpty()) {
            return;
        }

        JMeterVariables vars = context.getVariables();
        if (vars == null) {
            return;
        }

        String[] varNames = getVarNames().split(",");
        String[] pathExprs = getJsonPathExprs().split(",");
        String[] matchNums = getMatchNumbers().split(",");
        String[] defaultVals = getDefaultValues().split(",");

        for (int i = 0; i < varNames.length; i++) {
            String varName = varNames[i].trim();
            if (varName.isEmpty()) {
                continue;
            }

            String pathExpr = i < pathExprs.length ? pathExprs[i].trim() : "";
            String matchNumStr = i < matchNums.length ? matchNums[i].trim() : "1";
            String defaultVal = i < defaultVals.length ? defaultVals[i].trim() : "";

            if (pathExpr.isEmpty()) {
                continue;
            }

            int matchNum = 1;
            try {
                matchNum = Integer.parseInt(matchNumStr);
            } catch (NumberFormatException ignored) {}

            try {
                Object value = JsonPath.read(responseJson, pathExpr);
                if (value == null) {
                    vars.put(varName, defaultVal);
                } else if (value instanceof List) {
                    List<?> list = (List<?>) value;
                    if (list.isEmpty()) {
                        vars.put(varName, defaultVal);
                    } else {
                        if (matchNum == -1) {
                            // Extract all matches
                            for (int j = 0; j < list.size(); j++) {
                                vars.put(varName + "_" + (j + 1), String.valueOf(list.get(j)));
                            }
                            vars.put(varName + "_matchNr", String.valueOf(list.size()));
                        } else if (matchNum == 0) {
                            // Random match
                            int idx = random.nextInt(list.size());
                            vars.put(varName, String.valueOf(list.get(idx)));
                        } else {
                            // Specific index (1-based)
                            int idx = matchNum - 1;
                            if (idx >= 0 && idx < list.size()) {
                                vars.put(varName, String.valueOf(list.get(idx)));
                            } else {
                                vars.put(varName, defaultVal);
                            }
                        }
                    }
                } else {
                    // Single value returned
                    if (matchNum == -1) {
                        vars.put(varName + "_1", String.valueOf(value));
                        vars.put(varName + "_matchNr", "1");
                    } else {
                        vars.put(varName, String.valueOf(value));
                    }
                }
            } catch (Exception e) {
                log.warn("JSONPath extraction failed for variable {}: {}", varName, e.getMessage());
                vars.put(varName, defaultVal);
            }
        }
    }

    // --- Property Getters / Setters ---

    public String getVarNames() {
        return getPropertyAsString(VAR_NAMES);
    }

    public void setVarNames(String varNames) {
        setProperty(VAR_NAMES, varNames);
    }

    public String getJsonPathExprs() {
        return getPropertyAsString(JSON_PATH_EXPRS);
    }

    public void setJsonPathExprs(String jsonPathExprs) {
        setProperty(JSON_PATH_EXPRS, jsonPathExprs);
    }

    public String getMatchNumbers() {
        return getPropertyAsString(MATCH_NUMBERS);
    }

    public void setMatchNumbers(String matchNumbers) {
        setProperty(MATCH_NUMBERS, matchNumbers);
    }

    public String getDefaultValues() {
        return getPropertyAsString(DEFAULT_VALUES);
    }

    public void setDefaultValues(String defaultValues) {
        setProperty(DEFAULT_VALUES, defaultValues);
    }
}
