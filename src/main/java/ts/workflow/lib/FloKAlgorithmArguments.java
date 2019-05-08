package ts.workflow.lib;

import java.util.HashMap;
import java.util.List;

/**
 *
 {
 "mainclass":"ts.workflow.operator.UserClass",
 "input":[
 "in_path1",
 "in_path2"
 ],
 "output":[
 "out_path1",
 "outpath2"
 ],
 "parameters":[
 {
 "name":"Filter"
 },
 {
 "nickname":"过滤"
 },
 {
 "condition":"px<0.5"
 }
 ]
 }
 */

public class FloKAlgorithmArguments {

    public String mainclass;
    public String masterUrl;
    public List<String> input;
    public List<String> inputType;
    public List<String> output;
    public List<String> outputType;
    public HashMap<String, String> parameters;

    public List<String> getInputType() {
        return inputType;
    }

    public void setInputType(List<String> inputType) {
        this.inputType = inputType;
    }

    public List<String> getOutputType() {
        return outputType;
    }

    public void setOutputType(List<String> outputType) {
        this.outputType = outputType;
    }

    // TODO init parameter with schema
    // public HashMap<String, String> schema;
    public String getMainclass() {
        return mainclass;
    }

    public void setMainclass(String mainclass) {
        this.mainclass = mainclass;
    }

    public List<String> getInput() {
        return input;
    }

    public void setInput(List<String> input) {
        this.input = input;
    }

    public String getMasterUrl() {
        return masterUrl;
    }

    public void setMasterUrl(String masterUrl) {
        this.masterUrl = masterUrl;
    }

    public List<String> getOutput() {
        return output;
    }

    public void setOutput(List<String> output) {
        this.output = output;
    }

    public HashMap<String, String> getParameters() {
        return parameters;
    }

    public void setParameters(HashMap<String, String> parameters) {
        this.parameters = parameters;
    }

    @Override
    public String toString() {
        return "FloKAlgorithmArguments{" +
                "mainclass='" + mainclass + '\'' +
                ", input=" + input +
                ", inputType=" + inputType +
                ", output=" + output +
                ", outputType=" + outputType +
                ", parameters=" + parameters +
                '}';
    }
}
