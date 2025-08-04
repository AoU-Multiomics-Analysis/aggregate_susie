version 1.0



task AggregateSusie{
    input{
        Array[String] SusieParquets
        Int Memory
        String OutputPrefix
    }

    command <<<
    mkdir inputs
    for file in ~{sep=' ' SusieParquets}; do
       gcloud storage cp $file inputs/
    done
    readlink -f inputs/* >> filelist.txt

    Rscript /tmp/merge_susie.R --FilePaths filelist.txt  --OutputPrefix ~{OutputPrefix}
    >>>

    runtime {
        docker: "ghcr.io/aou-multiomics-analysis/aggregate_susie:main"
        disks: "local-disk 500 SSD"
        memory: "~{Memory}GB"
        cpu: "1"
    }
 


    output {
        File MergedSusieParquet = "${OutputPrefix}_SusieMerged.parquet" 
        File MergedSusieTsv = "${OutputPrefix}_SusieMerged.tsv.gz" 
    } 

}


workflow AggregateSusieWorkflow {
    input {
        Array[String] SusieParquets 
        Int Memory 
        String OutputPrefix
    }
    
    call AggregateSusie {
        input:
            SusieParquets = SusieParquets,
            OutputPrefix = OutputPrefix,
            Memory = Memory
    }
    output {
        File MergedSusieParquet = AggregateSusie.MergedSusieParquet
        File MergedSusieTsv = AggregateSusie.MergedSusieTsv
    }

}
