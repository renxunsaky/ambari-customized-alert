
import org.apache.kafka.common.utils.Utils;

public final class GetPartitionNumber {
	public static void main(String[] args) {
		System.out.println(getPartition(args[0], Integer.parseInt(args[1])));
	}

	public static int getPartition(String key, int partitionNb) {
		return Utils.toPositive(Utils.murmur2(key.getBytes())) % partitionNb;
	}
}
