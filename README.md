# QESM Kafka Broker Batching Simulation

[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/shumkar-barpiev/QESM_kafka_broker/blob/main/draw_plots.ipynb)

This Maven project uses the ORIS Sirio library to simulate a Stochastic Time
Petri Net (STPN) model of Kafka-style message batching. The model studies the
trade-off between batch size, timeout, push overhead, gateway backlog, service
backlog, batching-induced idle time, and service-rate stability.

The project is an abstraction of Kafka batching rather than an implementation
of Apache Kafka itself.

## Model overview

Messages arrive at `MsgsAtGateway` according to a Poisson process. A batch is
pushed to `AtService` when either:

- `MsgsAtGateway` reaches `BatchSize`; or
- the deterministic `Timeout` expires while the gateway contains messages.

Every push adds the configured `Overhead` to the service workload. Service time
is exponential, with rate:

```text
1 + Stability / 100
```

The main experiments keep `arrivalRate` fixed at `1`.

The watcher places `Idle`, `NotIdle`, `Empty`, and `NotEmpty` track whether the
service and gateway are empty. They support measurement of batching-induced
idle time: periods in which the service is idle while messages are still
waiting at the gateway.

## Project structure

```text
src/main/java/com/myexam/app/qesm/
├── App.java
├── analysis/
│   └── TransientSimulation.java
└── model/
    └── KafkaBrokerModel.java
```

- `KafkaBrokerModel` builds a parameterized Sirio Petri net and its initial
  marking.
- `TransientSimulation` runs one transient experiment, evaluates the selected
  reward type, and writes a CSV result.
- `App` runs the configured baseline, BatchSize, Timeout, and batching-idle
  experiments.

## Requirements

- Java 24
- Apache Maven

Sirio `2.0.5` is declared in `pom.xml` and is resolved by Maven.

## Build the project

From the project root, run:

```bash
mvn -DskipTests package
```

The packaged JAR is written to:

```text
target/qesm-0.0.1-SNAPSHOT.jar
```

## Run the configured experiments

Run `App` from an IDE, or use Maven:

```bash
mvn org.codehaus.mojo:exec-maven-plugin:3.5.0:java \
  -Dexec.mainClass=com.myexam.app.qesm.App
```

`App` runs these experiments:

| Experiment               | BatchSize | Timeout | Overhead | Stability | Reward type          |
| ------------------------ | --------: | ------: | -------: | --------: | -------------------- |
| Baseline queue behaviour |        20 |      25 |        2 |        30 | `NORMAL_TRANSIENT`   |
| BatchSize backlog        |        10 |      25 |        2 |        30 | `NORMAL_TRANSIENT`   |
| BatchSize backlog        |        30 |      25 |        2 |        30 | `NORMAL_TRANSIENT`   |
| Timeout backlog          |        20 |    12.5 |        2 |        30 | `NORMAL_TRANSIENT`   |
| Timeout backlog          |        20 |      35 |        2 |        30 | `NORMAL_TRANSIENT`   |
| Batching-induced idle    |        20 |      25 |        2 |        30 | `CUMULATIVE_WATCHER` |

The configured sampling step is `0.1`, the time horizon is `250`, and the run
count is `1`. A single run is intended for model and trajectory validation;
increase the run count for quantitative comparisons.

## Run one custom experiment

Use `TransientSimulation` directly when you want to supply a specific model
configuration:

```bash
mvn org.codehaus.mojo:exec-maven-plugin:3.5.0:java \
  -Dexec.mainClass=com.myexam.app.qesm.analysis.TransientSimulation \
  -Dexec.args="20 25 2 30 0.1 250 100 NORMAL_TRANSIENT"
```

The arguments are positional:

```text
BatchSize Timeout Overhead Stability [Step] [Horizon] [Runs] [RewardType]
```

| Argument     | Meaning                                                    |                                    Example |
| ------------ | ---------------------------------------------------------- | -----------------------------------------: |
| `BatchSize`  | Number of gateway messages that triggers an immediate push |                                       `20` |
| `Timeout`    | Maximum batching delay                                     |                                       `25` |
| `Overhead`   | Fixed service work added by each push                      |                                        `2` |
| `Stability`  | Percentage service-rate margin                             |                                       `30` |
| `Step`       | Transient sampling step                                    |                            `0.1` or `0.01` |
| `Horizon`    | Simulation time horizon                                    |                             `250` or `500` |
| `Runs`       | Number of stochastic replications                          |                      `1` or a larger value |
| `RewardType` | Reward calculation to perform                              | `NORMAL_TRANSIENT` or `CUMULATIVE_WATCHER` |

When the optional arguments are omitted, the defaults are step `0.1`, horizon
`250`, one run, and `NORMAL_TRANSIENT`.

## Reward types

### Normal transient rewards

`NORMAL_TRANSIENT` corresponds to:

```text
MsgsAtGateway;AtService
```

Its CSV columns are:

```text
time,meanMsgsAtGateway,meanAtService
```

Use it for baseline queue behaviour and the BatchSize, Timeout, and Stability
studies.

### Cumulative watcher rewards

`CUMULATIVE_WATCHER` corresponds to:

```text
Empty;If(Idle==1&&NotEmpty==1,2,0)
```

Its CSV columns are:

```text
time,cumulativeEmpty,cumulativeBatchingIdleReward,batchingIdleTime,batchingIdleFraction
```

The watcher reward is scaled by two, so:

```text
batchingIdleTime = cumulativeBatchingIdleReward / 2
batchingIdleFraction = batchingIdleTime / elapsedTime
```

The cumulative series are calculated numerically at the configured sampling
step. Use step `0.01` for detailed experiments.

## Outcome files

Results are stored in the root-level `outcomes` directory. The directory is
created automatically when it does not exist. An existing result with the same
filename is truncated and replaced, so rerunning an experiment never appends to
an old CSV.

Running `App` creates:

```text
outcomes/
├── baseline_queue_behaviour.csv
├── batch_size_10_backlog.csv
├── batch_size_30_backlog.csv
├── batching_induced_idle.csv
├── timeout_12_5_backlog.csv
└── timeout_35_backlog.csv
```

Running `TransientSimulation` directly creates a filename containing the reward
type and supplied model parameters.

## Draw the outcome plots

The root-level `draw_plots.ipynb` notebook reads the six configured CSV files
from `outcomes` and plots:

- baseline queue behaviour;
- the BatchSize effect on gateway and service backlog;
- the Timeout effect on gateway and service backlog; and
- cumulative batching-induced idle behaviour.

The plots are displayed inline in the notebook only.

## Open the plots in Google Colab

The file
[`draw_plots.ipynb`](https://github.com/shumkar-barpiev/QESM_kafka_broker/blob/main/draw_plots.ipynb)
reads the six configured CSV files from the `outcomes/` folder and creates the
baseline, BatchSize, Timeout, and cumulative batching-induced idle plots.

### Open directly

Click the **Open in Colab** badge near the top of this README, or use this link:

[Open `draw_plots.ipynb` in Google Colab](https://colab.research.google.com/github/shumkar-barpiev/QESM_kafka_broker/blob/main/draw_plots.ipynb)

### Open manually from Colab

1. Open [Google Colab](https://colab.research.google.com/).
2. Select **File → Open notebook**.
3. Select the **GitHub** tab.
4. Paste this complete GitHub URL into the search field:

   ```text
    https://github.com/shumkar-barpiev/kafka-message-broker-qesm/blob/main/draw_plots.ipynb
   ```

5. Press Enter and select `draw_plots.ipynb` from the result.

Use the complete URL. A partial path such as
`kafka-message-broker-qesm/blob/main/draw_plots.ipynb` may not return a result.

### Prepare the project files in Colab

Colab opens the notebook without cloning the complete Maven project. Before
running the notebook's plotting cells, add and run a temporary setup cell:

```python
!git clone https://github.com/shumkar-barpiev/QESM_kafka_broker.git
%cd QESM_kafka_broker
```

Confirm that the generated CSV files are available:

```python
!ls outcomes
```

If the CSV files are not stored in the repository, first run the Java
experiments locally and upload the resulting files to
`/content/QESM_kafka_broker/outcomes/` using Colab's Files panel. Then run the
remaining notebook cells in order. Colab already provides pandas and
matplotlib, so no additional Python package installation is normally required.
