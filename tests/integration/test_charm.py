#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Test slurmctld charm against other SLURM charms in the latest/edge channel."""

import logging
import pathlib
from typing import Any, Coroutine

import asyncio
import pytest
import tenacity
from pytest_operator.plugin import OpsTest

from helpers import get_slurmctld_res, get_slurmd_res

logger = logging.getLogger(__name__)

SERIES = ["focal"]
SLURMCTLD = "slurmctld"
SLURMD = "slurmd"
SLURMDBD = "slurmdbd"
DATABASE = "percona-cluster"


@pytest.mark.abort_on_fail
@pytest.mark.skip_if_deployed
@pytest.mark.parametrize("series", SERIES)
async def test_build_and_deploy_against_edge(
    ops_test: OpsTest, slurmctld_charm: Coroutine[Any, Any, pathlib.Path], series: str
) -> None:
    """Test that the slurmctld charm can stabilize against slurmd, slurmdbd, and percona."""
    logger.info(f"Deploying {SLURMCTLD} against {SLURMD}, {SLURMDBD}, and {DATABASE}...")
    slurmctld_res = get_slurmctld_res()
    slurmd_res = get_slurmd_res()
    await asyncio.gather(
        ops_test.model.deploy(
            str(await slurmctld_charm),
            application_name=SLURMCTLD,
            num_units=1,
            resources=slurmctld_res,
            series=series,
        ),
        ops_test.model.deploy(
            SLURMD,
            application_name=SLURMD,
            channel="edge",
            num_units=1,
            resources=slurmd_res,
            series=series,
        ),
        ops_test.model.deploy(
            SLURMDBD,
            application_name=SLURMDBD,
            channel="edge",
            num_units=1,
            series=series,
        ),
        ops_test.model.deploy(
            DATABASE,
            application_name=DATABASE,
            channel="edge",
            num_units=1,
            series="bionic",
        ),
    )
    # Attach resources to charms.
    await ops_test.juju("attach-resource", SLURMCTLD, f"etcd={slurmctld_res['etcd']}")
    await ops_test.juju("attach-resource", SLURMD, f"nhc={slurmd_res['nhc']}")
    # Set integrations for charmed applications.
    await ops_test.model.relate(f"{SLURMCTLD}:{SLURMD}", f"{SLURMD}:{SLURMD}")
    await ops_test.model.relate(f"{SLURMCTLD}:{SLURMDBD}", f"{SLURMDBD}:{SLURMDBD}")
    await ops_test.model.relate(f"{SLURMDBD}:db", f"{DATABASE}:db")
    # Reduce the update status frequency to accelerate the triggering of deferred events.
    async with ops_test.fast_forward():
        await ops_test.model.wait_for_idle(apps=[SLURMCTLD], status="active", timeout=1000)
        assert ops_test.model.applications[SLURMCTLD].units[0].workload_status == "active"


@pytest.mark.abort_on_fail
@tenacity.retry(
    wait=tenacity.wait.wait_exponential(multiplier=2, min=1, max=30),
    stop=tenacity.stop_after_attempt(3),
    reraise=True,
)
async def test_slurmctld_is_active(ops_test: OpsTest) -> None:
    """Test that slurmctld is active inside Juju unit."""
    logger.info("Checking that slurmctld is active inside Juju unit...")
    slurmctld_unit = ops_test.model.applications[SLURMCTLD].units[0]
    res = (await slurmctld_unit.ssh("systemctl is-active slurmctld")).strip("\n")
    assert res == "active"


@pytest.mark.abort_on_fail
@tenacity.retry(
    wait=tenacity.wait.wait_exponential(multiplier=2, min=1, max=30),
    stop=tenacity.stop_after_attempt(3),
    reraise=True,
)
async def test_slurmctld_port_listen(ops_test: OpsTest) -> None:
    """Test that slurmctld is listening on port 6817."""
    logger.info("Checking that slurmctld is listening on port 6817...")
    slurmctld_unit = ops_test.model.applications[SLURMCTLD].units[0]
    res = await slurmctld_unit.ssh("sudo lsof -t -n -iTCP:6817 -sTCP:LISTEN")
    assert res != ""


@pytest.mark.abort_on_fail
@tenacity.retry(
    wait=tenacity.wait.wait_exponential(multiplier=2, min=1, max=30),
    stop=tenacity.stop_after_attempt(3),
    reraise=True,
)
async def test_etcd_is_active(ops_test: OpsTest) -> None:
    """Test that etcd is active inside Juju unit."""
    logger.info("Checking that etcd is active inside Juju unit...")
    slurmctld_unit = ops_test.model.applications[SLURMCTLD].units[0]
    res = (await slurmctld_unit.ssh("systemctl is-active etcd")).strip("\n")
    assert res == "active"


@pytest.mark.abort_on_fail
@tenacity.retry(
    wait=tenacity.wait.wait_exponential(multiplier=2, min=1, max=30),
    stop=tenacity.stop_after_attempt(3),
    reraise=True,
)
async def test_munge_is_active(ops_test: OpsTest) -> None:
    """Test that munge is active inside Juju unit."""
    logger.info("Checking that munge is active inside Juju unit...")
    slurmctld_unit = ops_test.model.applications[SLURMCTLD].units[0]
    res = (await slurmctld_unit.ssh("systemctl is-active munge")).strip("\n")
    assert res == "active"
