package org.ipvp.bungeesync;

import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/**
 * Represents a zPermissions group entity
 */
public class Group {

    private final int id;
    private final String name;
    private final int priority;
    private Group parent;
    private Set<Permission> permissions = new HashSet<>();

    public Group(int id, String name, int priority) {
        this.id = id;
        this.name = name;
        this.priority = priority;
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
        this.parent = parent;
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
