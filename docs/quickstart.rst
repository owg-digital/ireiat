Quickstart
==========

Run the data pipeline, which locally saves artifacts needed to solve the TAP across modes.

.. code-block::

   dagster job execute -m ireiat.data_pipeline -j all --config some_config.yaml

Solve a traffic assignment problem for a particular mode (highway, rail, or marine).

.. code-block::

   ireiat solve -m rail

Example output looks like:

.. csv-table:: Example traffic output parquet file
   :file: _static/traffic_example.csv
   :widths: 30, 30, 30, 30, 30, 30, 30, 30
   :header-rows: 1

Create postprocessing artifacts once the solution files exist.

.. code-block::

   ireiat postprocess -m rail

Postprocessing takes the traffic data with the relevant network and plots
utilization across the network, which can highlight areas of congestion.

..  image:: ../_static/rail_traffic.png

You can alter demand or network parameters in the configuration file to
understand sensitivities (e.g. capacity reduction, capacity expansion,
congestion impacts, free flow times, etc.).
