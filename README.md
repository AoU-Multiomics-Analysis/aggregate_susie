# aggregate_susie

Workflow to aggregate and annotate the individual gene-level results of the [SuSiE](https://stephenslab.github.io/susieR/) fine-mapping workflow.

## Overview

This repository provides two WDL workflows and two R scripts for:
1. **Merging** per-gene SuSiE parquet output files into a single aggregated file.
2. **Annotating** the merged fine-mapping results with a variety of external functional and population-genomic data sources.

---

## Workflows

### `merge_susie.wdl` — `AggregateSusieWorkflow`

The primary end-to-end workflow. It runs both the aggregation and annotation steps in sequence.

**Inputs**

| Parameter | Type | Description |
|---|---|---|
| `SusieParquetsFOFN` | File | File-of-file-names (one GCS path per line) pointing to per-gene SuSiE parquet output files |
| `Memory` | Int | Memory (GB) to allocate to each task |
| `NumThreads` | Int | CPU threads to allocate |
| `OutputPrefix` | String | Prefix for all output files |
| `AggregateMode` | String | Type of SuSiE output to aggregate — either `pip` (posterior inclusion probability table) or `lbf` (log Bayes factors table) |
| `GencodeGTF` | File | GENCODE GTF annotation file (hg38) |
| `PlinkAfreq` | File | PLINK `.afreq` allele-frequency file for the study cohort |
| `AnnotationPhyloP` | File | PhyloP conservation scores BigWig file |
| `AnnotationENCODE` | File | ENCODE candidate cis-regulatory elements (cCREs) BED file |
| `AnnotationFANTOM5` | File | FANTOM5 enhancer/promoter BED file (hg38, from UCSC genome browser) |
| `AnnotationVEP` | File | Tabix-indexed VEP consequence annotation table |
| `AnnotationVEPIndex` | File | Tabix index (`.tbi`) for the VEP annotation table |
| `AnnotationGnomad` | File | gnomAD gene constraint table (TSV with `gene_id` and `lof.pLI` columns) |

**Outputs**

| File | Description |
|---|---|
| `AnnotatedMergedSusieParquet` | TSV file containing merged and fully annotated fine-mapping results |

---

### `workflows/annotate_susie.wdl` — `AnnotateSusie`

A standalone workflow for annotating an already-merged SuSiE TSV. This is useful when re-annotating results without re-running aggregation.

**Inputs**

| Parameter | Type | Description |
|---|---|---|
| `SusieTSV` | File | Merged SuSiE TSV (e.g., output of the aggregation step) |
| `Memory` | Int | Memory (GB) to allocate |
| `NumThreads` | Int | CPU threads (accepted as a workflow input but the task currently runs on a single CPU; any positive integer is valid) |
| `OutputPrefix` | String | Prefix for output files |
| `GencodeGTF` | File | GENCODE GTF annotation file (hg38) |
| `PlinkAfreq` | File | PLINK `.afreq` allele-frequency file |
| `AnnotationPhyloP` | File | PhyloP BigWig file |
| `AnnotationENCODE` | File | ENCODE cCREs BED file |
| `AnnotationFANTOM5` | File | FANTOM5 BED file |
| `AnnotationVEP` | File | Tabix-indexed VEP annotation table |
| `AnnotationVEPIndex` | File | Tabix index for VEP table |
| `AnnotationGnomad` | File | gnomAD gene constraint table |

**Outputs**

| File | Description |
|---|---|
| `AnnotatedMergedSusieTSV` | Annotated TSV file |

---

## Scripts

### `merge_susie.R`

Reads a list of per-gene SuSiE parquet files and concatenates them into a single merged file.

**Command-line arguments**

| Argument | Description |
|---|---|
| `--FilePaths` | Path to a plain-text file listing the local paths of SuSiE parquet files (one per line) |
| `--OutputPrefix` | Prefix for the output filenames |
| `--SusieType` | Type of SuSiE output to merge — `pip` or `lbf` |

**Outputs**
- `<OutputPrefix>_SusieMerged.parquet` — merged results in Apache Parquet format
- `<OutputPrefix>_SusieMerged.tsv.gz` — merged results as a gzip-compressed TSV

---

### `annotate_susie_data.R`

Annotates a merged SuSiE TSV with the following data sources:

- **Allele frequencies** from a PLINK `.afreq` file; variants with `ALT_FREQS > 0.5` are strand-flipped so that `ref`/`alt` and `posterior_mean` always reflect the minor allele.
- **GENCODE GTF** — gene biotype, gene name, and TSS distance (`distTSS`).
- **ENCODE cCREs** — regulatory element classification (`PLS`, `pELS`, `dELS`, etc.).
- **FANTOM5** — promoter/enhancer overlap flag.
- **VEP consequence annotations** — queried via tabix; overlapping annotations are one-hot encoded as logical columns.
- **PhyloP conservation scores** — per-variant score extracted from a BigWig file.
- **gnomAD gene constraint** — `lof.pLI` and a derived `Constrained`/`Unconstrained` label (threshold: pLI > 0.9).

**Command-line arguments**

| Argument | Description |
|---|---|
| `--SusieTSV` | Path to the merged SuSiE TSV file |
| `--GencodeGTF` | Path to GENCODE GTF |
| `--OutputPrefix` | Prefix for output files |
| `--PlinkAfreq` | Path to PLINK `.afreq` file |
| `--ENCODEcCRES` | Path to ENCODE cCREs BED file |
| `--VEPAnnotationsTable` | Path to tabix-indexed VEP annotation table |
| `--gnomadConstraint` | Path to gnomAD gene constraint TSV |
| `--phyloPBigWig` | Path to PhyloP BigWig file |
| `--FANTOM5` | Path to FANTOM5 BED file |

**Output**
- `<OutputPrefix>_SusieMerged.annotated.tsv` — fully annotated fine-mapping results

---

## Data Preparation

Before running the workflows, the following input files need to be prepared or downloaded.

### SuSiE parquet files (FOFN)
The `SusieParquetsFOFN` input should be a plain-text file with one Google Cloud Storage (GCS) path per line pointing to per-gene SuSiE output parquet files, e.g.:
```
gs://my-bucket/susie_results/gene1_pip.parquet
gs://my-bucket/susie_results/gene2_pip.parquet
```

### GENCODE GTF
Download the hg38 GENCODE annotation:
```bash
wget https://ftp.ebi.ac.uk/pub/databases/gencode/Gencode_human/release_44/gencode.v44.annotation.gtf.gz
```

### PLINK allele frequency file
Generate using PLINK 2 on the study genotype data:
```bash
plink2 --bfile <your_genotypes> --freq --out <OutputPrefix>
```
The resulting `.afreq` file must contain `ID` and `ALT_FREQS` columns.

### ENCODE cCREs
Download the hg38 ENCODE candidate cis-regulatory elements (cCREs):
```bash
wget https://downloads.wenglab.org/V3/GRCh38-cCREs.bed
```
The file is expected to be a BED file where column 6 contains a comma-separated list of cCRE types (`PLS`, `pELS`, `dELS`, etc.).

### FANTOM5
Download the hg38 FANTOM5 permissive enhancer peak file from the UCSC genome browser or directly from FANTOM5:
```bash
wget https://fantom.gsc.riken.jp/5/datafiles/reprocessed/hg38_latest/extra/enhancer/F5.hg38.enhancers.bed.gz
```

### VEP annotation table
A tabix-indexed TSV summarising VEP consequences. Each row should describe one variant with columns: `chromosome`, `start`, `ref`, `alt`, and at least one consequence column. Index with:
```bash
bgzip VEP_annotations.tsv
tabix -s 1 -b 2 -e 2 VEP_annotations.tsv.gz
```

### PhyloP BigWig
Download the UCSC hg38 PhyloP 100-way conservation BigWig:
```bash
wget https://hgdownload.soe.ucsc.edu/goldenPath/hg38/phyloP100way/hg38.phyloP100way.bw
```

### gnomAD gene constraint
Download the gnomAD v2.1 constraint table:
```bash
wget https://storage.googleapis.com/gcp-public-data--gnomad/release/2.1.1/constraint/gnomad.v2.1.1.lof_metrics.by_gene.txt.bgz
```
The table must contain `gene_id`, `canonical`, and `lof.pLI` columns. The `canonical` column is used to filter to canonical transcripts only before joining.

---

## Docker

The workflows use the Docker image `ghcr.io/aou-multiomics-analysis/aggregate_susie:main`, which is built automatically from the `Dockerfile` in this repository via GitHub Actions on every push to `main`.

The image is based on [pixi](https://prefix.dev/) and installs:
- R 4.4 with `tidyverse`, `data.table`, `arrow`, `optparse`, `bedr`, `plyranges`, and `rtracklayer`
- `tabix` (for VEP annotation queries)
- Google Cloud SDK (for `gsutil` file localisation in the aggregation task)

