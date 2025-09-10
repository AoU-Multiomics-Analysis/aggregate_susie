FROM mambaorg/micromamba:1.5.3

## Set up environment
ENV MAMBA_DOCKERFILE_ACTIVATE=1
ENV PATH=/opt/conda/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
## Create a new environment and install packages

RUN micromamba install -y -n base -c conda-forge -c bioconda \
    conda-forge::r-tidyverse \
    conda-forge::datatable \
    conda-forge::r-optparse \
    conda-forge::r-optparse \
    bioconda::bioconductor-rtracklayer \
    conda-forge::r-arrow \
    bioconda::bioconductor-plyranges \
    conda-forge::google-cloud-sdk

COPY merge_susie.R .
COPY annotate_susie_data.R . 

CMD ["bash"]
 
