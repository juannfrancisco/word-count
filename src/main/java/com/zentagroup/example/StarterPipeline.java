/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.zentagroup.example;

import com.zentagroup.example.config.WordCountOptions;
import com.zentagroup.example.functions.FormatAsTextFn;
import com.zentagroup.example.transforms.CountWords;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class StarterPipeline {
  private static final Logger LOG = LoggerFactory.getLogger(StarterPipeline.class);

  public static void main(String[] args) {


    // Crea un objeto de opciones para añadir parametros al inicio del flujo (archivo de entrada y salida)
    WordCountOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(WordCountOptions.class);

    // Crea el pipeline a partir de las opciones definidas.
    Pipeline p = Pipeline.create(options);

    // Primera ejecucion del pipeline para leer un archivo desde cloud storage (bucket)
    p.apply("ReadLines", TextIO.read().from(options.getInputFile()))

    // Funcion que cuenta la coincidencia de palabras en el texto completo
    .apply("Contar Lineas", new CountWords())

    // Transforma la pcollection KV a un pcollection String.
    .apply("Formato de salida", MapElements.via(new FormatAsTextFn()))

    // Ultima Escribe la pcollection de string en archivos dentro de cloud storage
    .apply("WriteCounts", TextIO.write().to(options.getOutput()));

    p.run().waitUntilFinish();


  }
}
