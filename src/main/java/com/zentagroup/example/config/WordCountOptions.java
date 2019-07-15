package com.zentagroup.example.config;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;


/**
 * Define las propiedas que se requieran pasar como
 * parametros al proceso de ETL
 */
public interface WordCountOptions extends PipelineOptions {



    @Description("Path of the file to read from") // Descripcion de la opcion
    @Default.String("gs://apache-beam-samples/shakespeare/kinglear.txt") // Valor por defecto del parametro
    // --inputFile
    String getInputFile();

    void setInputFile(String value);

    /** Set this required option to specify where to write the output. */
    @Description("Path of the file to write to")  // Descripcion de la opcion
    @Validation.Required // Validar que sea requerido
    // --output
    String getOutput();

    void setOutput(String value);
  }