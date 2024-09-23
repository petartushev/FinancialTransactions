package serializers;

import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.sql.Connection;

public class NoOpConnectionSerializer extends TypeSerializer<Connection> {

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public TypeSerializer<Connection> duplicate() {
        return this;
    }

    @Override
    public Connection createInstance() {
        return null;
    }

    @Override
    public Connection copy(Connection from) {
        return from;
    }

    @Override
    public Connection copy(Connection from, Connection reuse) {
        return from;
    }

    @Override
    public int getLength() {
        return 0;
    }

    @Override
    public void serialize(Connection record, DataOutputView target) throws IOException {
        // No-op, nothing to serialize
    }

    @Override
    public Connection deserialize(DataInputView source) throws IOException {
        return null;
    }

    @Override
    public Connection deserialize(Connection reuse, DataInputView source) throws IOException {
        return null;
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        // No-op, nothing to copy
    }

    @Override
    public boolean equals(Object o) {
        return false;
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public TypeSerializerSnapshot<Connection> snapshotConfiguration() {
        return new NoOpConnectionSerializerSnapshot();
    }
}

class NoOpConnectionSerializerSnapshot extends SimpleTypeSerializerSnapshot<Connection> {
    public NoOpConnectionSerializerSnapshot() {
        super(() -> new NoOpConnectionSerializer());
    }
}