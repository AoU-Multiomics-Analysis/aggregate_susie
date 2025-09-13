version 1.0 

task AnnotateSusieTask {
    input {
        File SusieParquet 
        File GencodeGTF
        File PlinkAfreq
        String OutputPrefix
        Int Memory
        File AnnotationENCODE 
        File AnnotationFANTOM5 
        File AnnotationVEP
        File AnnotationVEPIndex 
        File AnnotationGnomad 
        File AnnotationPhyloP 
      

    }
    command <<<
    Rscript /tmp/annotate_susie_data.R \
        --OutputPrefix ~{OutputPrefix} \
        --GencodeGTF ~{GencodeGTF} \
        --PlinkAfreq ~{PlinkAfreq} \
        --SusieParquet ~{SusieParquet} \
        --phyloPBigWig ~{AnnotationPhyloP} \
        --FANTOM5 ~{AnnotationFANTOM5} \
        --gnomadConstraint ~{AnnotationGnomad} \
        --ENCODEcCRES ~{AnnotationENCODE} \
        --VEPAnnotationsTable ~{AnnotationVEP}
    >>>
   runtime {
        docker: "ghcr.io/aou-multiomics-analysis/aggregate_susie:main"
        disks: "local-disk 500 SSD"
        memory: "~{Memory}GB"
        cpu: "1"
    }


    output {
        File AnnotatedSusieParquetOut = "~{OutputPrefix}_SusieMerged.annotated.parquet" 
    }

}


workflow AnnotateSusie {
    input {
        #Array[File] SusieParquets
        File SusieParquet
        Int Memory 
        String OutputPrefix
        Int NumThreads

        File GencodeGTF 
        File PlinkAfreq
        File AnnotationPhyloP 
        File AnnotationENCODE 
        File AnnotationFANTOM5 
        File AnnotationVEP 
        File AnnotationGnomad
        File AnnotationVEPIndex 
        
    }
    call AnnotateSusieTask {
        input:
            SusieParquet = SusieParquet,
            GencodeGTF = GencodeGTF,
            PlinkAfreq = PlinkAfreq,
            OutputPrefix = OutputPrefix,
            Memory = Memory,
            AnnotationPhyloP = AnnotationPhyloP,
            AnnotationENCODE = AnnotationENCODE,
            AnnotationFANTOM5 = AnnotationFANTOM5,
            AnnotationVEP = AnnotationVEP,
            AnnotationVEPIndex = AnnotationVEPIndex,
            AnnotationGnomad = AnnotationGnomad
    } 

    output {
        File AnnotatedMergedSusieParquet = AnnotateSusieTask.AnnotatedSusieParquetOut
       }
    }
