package org.corfudb.infrastructure.paxos;

import org.corfudb.infrastructure.Rank;
import org.corfudb.infrastructure.paxos.Paxos.OperationResult;
import org.corfudb.infrastructure.paxos.PaxosDataStore.InMemoryPaxosDataStore;
import org.corfudb.protocols.wireprotocol.LayoutProposeRequest;
import org.corfudb.protocols.wireprotocol.LayoutProposeResponse;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.Layout.LayoutSegment;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Optional;
import java.util.UUID;

class PaxosTest {

    @Test
    public void testSuccessfulCase_NotRaceCondition_oneClientUpdates() {
        //test various dead/live locks.
        //может ли произойти деградация консенсуса с фазы2 на фазу1?;
        //Когда первый записал фазу1 успешно и находится на второй фазе а;
        //второй разъебал его записав больший ранк в первой фазе;
        //(корректно ли мое добавленное условие что если фаза 2 существует то фаза 1 не может быть выполнена?)

        //попробовать заимплементить лайв лок когда протокол застревает когда два клиента до усрачки обновляют ранки;
        //и не дают друг другу иметь прогресс;

        PaxosDataStore ds = new InMemoryPaxosDataStore();
        Paxos node1Paxos = Paxos.builder().dataStore(ds).build();

        UUID clusterId = UUID.fromString("10000000-0000-0000-0000-000000000000");

        UUID client1Id = UUID.fromString("00000000-0000-0000-0000-000000000001");

        Rank prepareRank = new Rank(1L, client1Id);
        //Rank prepareRankSecond = new Rank(2L, client1Id);
        OperationResult prepare = node1Paxos.prepare(prepareRank);
        LayoutSegment segment = new LayoutSegment(
                Layout.ReplicationMode.CHAIN_REPLICATION,
                0L,
                -1L,
                Arrays.asList(new Layout.LayoutStripe(Arrays.asList("a")))
        );

        Layout layout = new Layout(
                Arrays.asList("a"),
                Arrays.asList("a"),
                Arrays.asList(segment),
                Arrays.asList(),
                0,
                clusterId
        );
        Optional<LayoutProposeResponse> propose = node1Paxos.propose(
                new LayoutProposeRequest(0L, 1L, layout),
                client1Id
        );

        System.out.println("1. prepare: " + prepare);
        System.out.println("2. propose: " + propose.get());
    }

}