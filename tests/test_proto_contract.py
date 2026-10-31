"""Contract checks for the single canonical gRPC interface."""

from primegrid import primegrid_pb2 as pb


def test_contract_has_two_services_and_ten_rpcs() -> None:
    master = pb.DESCRIPTOR.services_by_name["MasterService"]
    worker = pb.DESCRIPTOR.services_by_name["WorkerService"]

    assert len(master.methods) == 8
    assert len(worker.methods) == 2
    assert {method.name for method in master.methods} == {
        "SubmitJob",
        "GetJobStatus",
        "WatchJob",
        "GetResult",
        "CancelJob",
        "ListWorkers",
        "RegisterWorker",
        "Heartbeat",
    }
    assert {method.name for method in worker.methods} == {"ProcessChunk", "Ping"}


def test_watch_job_is_server_streaming() -> None:
    method = pb.DESCRIPTOR.services_by_name["MasterService"].methods_by_name["WatchJob"]

    assert method.server_streaming is True
    assert method.client_streaming is False


def test_every_request_type_is_unique_and_contains_required_ids() -> None:
    requests = []
    for service in pb.DESCRIPTOR.services_by_name.values():
        for method in service.methods:
            requests.append(method.input_type.full_name)

    assert len(requests) == len(set(requests))
    assert "request_id" in pb.SubmitJobRequest.DESCRIPTOR.fields_by_name
    assert "request_id" not in pb.GetJobStatusRequest.DESCRIPTOR.fields_by_name
    assert "chunk_id" in pb.ProcessChunkRequest.DESCRIPTOR.fields_by_name
