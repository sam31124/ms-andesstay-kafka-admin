package cl.duoc.andesstay.kafka.dto;

public class ConsumerGroupInfo {

    private String groupId;
    private String state;
    private int membersCount;
    private boolean simpleConsumerGroup;

    public ConsumerGroupInfo() {
    }

    public ConsumerGroupInfo(String groupId, String state, int membersCount, boolean simpleConsumerGroup) {
        this.groupId = groupId;
        this.state = state;
        this.membersCount = membersCount;
        this.simpleConsumerGroup = simpleConsumerGroup;
    }

    public String getGroupId() { return groupId; }
    public void setGroupId(String groupId) { this.groupId = groupId; }

    public String getState() { return state; }
    public void setState(String state) { this.state = state; }

    public int getMembersCount() { return membersCount; }
    public void setMembersCount(int membersCount) { this.membersCount = membersCount; }

    public boolean isSimpleConsumerGroup() { return simpleConsumerGroup; }
    public void setSimpleConsumerGroup(boolean simpleConsumerGroup) { this.simpleConsumerGroup = simpleConsumerGroup; }
}
