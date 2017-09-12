package org.ipvp.bungeesync;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import net.md_5.bungee.api.connection.ProxiedPlayer;

/**
 * Represents a zPermissions group entity
 */
public class Group {

    private final int id;
    private final String name;
    private final int priority;
    private Group parent;
    private Set<Group> children = new HashSet<>();
    private Set<Permission> permissions = new HashSet<>();
    private Set<UUID> players = Collections.newSetFromMap(new ConcurrentHashMap<>());

    public Group(int id, String name, int priority) {
        this.id = id;
        this.name = name;
        this.priority = priority;
        permissions.add(new Permission("group." + name, true));
    }

    public int getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public int getPriority() {
        return priority;
    }

    public Group getParent() {
        return parent;
    }

    public void setParent(Group parent) {
        if (this.parent != null) {
            this.parent.children.remove(this);
        }
        this.parent = parent;
        if (this.parent != null) {
            this.parent.children.add(this);
        }
    }
    
    public Set<Group> getChildren() {
        return Collections.unmodifiableSet(children);
    }

    public Set<Permission> getPermissions() {
        return Collections.unmodifiableSet(permissions);
    }

    public void addPermission(Permission permission) {
        permissions.add(permission);
    }

    public void removePermission(Permission permission) {
        permissions.remove(permission);
    }
    
    public void addPlayer(UUID player) {
        players.add(player);
    }
    
    public void removePlayer(UUID player) {
        players.remove(player);
    }
    
    public Set<UUID> getPlayers() {
        return Collections.unmodifiableSet(players);
    }

    @Override
    public int hashCode() {
        return 31 * Integer.hashCode(id) * name.hashCode() * Integer.hashCode(priority);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Group)) {
            return false;
        } else if (o == this) {
            return true;
        }
        Group other = (Group) o;
        return other.id == id && other.name.equals(name)
                && other.priority == priority
                && other.permissions.equals(permissions);
    }
}
