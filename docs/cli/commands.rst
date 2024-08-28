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

Application-specific commands
=============================

Once the package is installed, any of these commands can be run with: :code:`irieat <cmd>` from the
command line (where :code:`<cmd>` is one of the commands listed below).

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
