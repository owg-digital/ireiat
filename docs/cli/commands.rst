============================
Command line interface (CLI)
============================

Non-application-specific commands
=================================

Data pipeline
-------------

We have leveraged `dagster <https://dagster.io/>`_ to run data pipelines, and the command to
initiate the dagster UI is below. Once the UI is started, individual assets or jobs can be run;
and running :code:`dagster --help` should provide the needed CLI options. Note that the Dagster
UI can be started with :code:`dagster dev`.

Configuration parameters are available by passing a yaml file to the run of the data pipeline.

A subset of a yaml configuration file is shown below:

.. code-block:: yaml

    ops:
      county_to_county_highway_tons:
        config:
          faf_demand_field: tons_2022
      faf5_truck_demand:
        config:
          faf_demand_field: tons_2022
          unknown_mode_percent: 0.3

See the repo under :code:`data/config.yaml` for a full example of all configurable parameters for
the data pipeline.

Application-specific commands
=============================

Once the package is installed, any of these commands can be run with: :code:`irieat <cmd>` from the
command line (where :code:`<cmd>` is one of the commands listed below).

Clearing the local cache
------------------------

The data pipeline downloads file from publicly available sources and keeps
them in a local cache. To delete the cache, including all intermediate artifacts, run the following:

.. click:: ireiat.run:clear_cache
  :prog: clear_cache
  :nested: full

Solving
-------

.. click:: ireiat.run:solve
   :prog: solve
   :nested: full

Postprocessing
--------------

.. click:: ireiat.run:postprocess
   :prog: postprocess
   :nested: full
