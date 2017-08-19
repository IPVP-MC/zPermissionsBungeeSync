package org.ipvp.bungeesync;

/**
 * Represents a permission in the zPermissions database
 */
public class Permission {
    
    private final String permission;
    private final boolean value;
    
    public Permission(String permission, boolean value) {
        this.permission = permission;
        this.value = value;
    }

    public String getPermission() {
        return permission;
    }

    public boolean getValue() {
        return value;
    }

    @Override
    public int hashCode() {
        return 31 * Boolean.hashCode(value) * permission.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Permission)) {
            return false;
        } else if (o == this) {
            return true;
        }
        Permission other = (Permission) o;
        return other.permission.equals(permission) && other.value == value;
    }
}
