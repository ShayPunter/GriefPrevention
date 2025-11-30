/*
    GriefPrevention Server Plugin for Minecraft
    Copyright (C) 2012 Ryan Hamshire

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package me.ryanhamshire.GriefPrevention.storage;

import com.google.common.io.Files;
import me.ryanhamshire.GriefPrevention.CustomLogEntryTypes;
import me.ryanhamshire.GriefPrevention.GriefPrevention;
import org.bukkit.Bukkit;
import org.bukkit.Location;
import org.bukkit.World;
import org.bukkit.configuration.file.YamlConfiguration;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.*;
import java.util.regex.Pattern;

/**
 * Utility class to migrate data from the old per-file format to the new
 * compact consolidated format.
 */
public class StorageMigrator
{
    private static final Pattern UUID_PATTERN = Pattern.compile("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}");

    private final Path dataFolder;
    private final CompactClaimStore claimStore;
    private final CompactPlayerStore playerStore;

    public StorageMigrator(Path dataFolder, CompactClaimStore claimStore, CompactPlayerStore playerStore)
    {
        this.dataFolder = dataFolder;
        this.claimStore = claimStore;
        this.playerStore = playerStore;
    }

    /**
     * Check if migration from old format is needed.
     */
    public boolean needsMigration()
    {
        Path claimDataFolder = dataFolder.resolve("ClaimData");
        Path playerDataFolder = dataFolder.resolve("PlayerData");

        // Check for old-style claim files (individual .yml files)
        if (java.nio.file.Files.exists(claimDataFolder))
        {
            File[] files = claimDataFolder.toFile().listFiles((dir, name) ->
                name.endsWith(".yml") && !name.startsWith("_"));
            if (files != null && files.length > 0)
            {
                return true;
            }
        }

        // Check for old-style player files (no extension)
        if (java.nio.file.Files.exists(playerDataFolder))
        {
            File[] files = playerDataFolder.toFile().listFiles((dir, name) ->
                !name.startsWith("$") && !name.startsWith("_") && !name.endsWith(".ignore"));
            if (files != null && files.length > 0)
            {
                // Check if at least one looks like a UUID
                for (File file : files)
                {
                    if (UUID_PATTERN.matcher(file.getName()).matches())
                    {
                        return true;
                    }
                }
            }
        }

        return false;
    }

    /**
     * Migrate all data from old format to new compact format.
     * Returns true if migration was successful.
     */
    public boolean migrate()
    {
        GriefPrevention.AddLogEntry("Starting migration to compact storage format...");

        try
        {
            int claimsMigrated = migrateClaims();
            int playersMigrated = migratePlayers();
            int groupsMigrated = migrateGroups();

            // Save the migrated data
            claimStore.saveAll();
            playerStore.saveAll();

            // Backup old files
            backupOldFiles();

            GriefPrevention.AddLogEntry("Migration complete! Migrated " + claimsMigrated + " claims, " +
                playersMigrated + " players, and " + groupsMigrated + " groups.");

            return true;
        }
        catch (Exception e)
        {
            GriefPrevention.AddLogEntry("Migration failed: " + e.getMessage(), CustomLogEntryTypes.Exception);
            e.printStackTrace();
            return false;
        }
    }

    /**
     * Migrate claims from individual YAML files to compact storage.
     */
    private int migrateClaims() throws IOException
    {
        Path claimDataFolder = dataFolder.resolve("ClaimData");
        if (!java.nio.file.Files.exists(claimDataFolder))
        {
            return 0;
        }

        List<World> validWorlds = Bukkit.getServer().getWorlds();
        File[] files = claimDataFolder.toFile().listFiles((dir, name) ->
            name.endsWith(".yml") && !name.startsWith("_"));

        if (files == null || files.length == 0)
        {
            return 0;
        }

        int migratedCount = 0;
        long maxClaimId = 0;

        // First pass: load all parent claims
        Map<Long, CompactClaimStore.ClaimData> allClaims = new HashMap<>();

        for (File file : files)
        {
            try
            {
                long claimId = Long.parseLong(file.getName().replace(".yml", ""));
                CompactClaimStore.ClaimData claimData = parseYamlClaimFile(file, claimId, validWorlds);

                if (claimData != null)
                {
                    allClaims.put(claimId, claimData);
                    if (claimId > maxClaimId)
                    {
                        maxClaimId = claimId;
                    }
                    migratedCount++;
                }
            }
            catch (NumberFormatException e)
            {
                GriefPrevention.AddLogEntry("Skipping non-numeric claim file: " + file.getName(), CustomLogEntryTypes.Debug);
            }
            catch (Exception e)
            {
                GriefPrevention.AddLogEntry("Error migrating claim file " + file.getName() + ": " + e.getMessage(), CustomLogEntryTypes.Exception);
            }
        }

        // Add all claims to the compact store
        for (CompactClaimStore.ClaimData claimData : allClaims.values())
        {
            claimStore.markDirty(createTempClaimForStorage(claimData));
        }

        // Update next claim ID
        claimStore.setNextClaimId(maxClaimId + 1);

        // Migrate the nextClaimID file if it exists
        Path nextIdFile = claimDataFolder.resolve("_nextClaimID");
        if (java.nio.file.Files.exists(nextIdFile))
        {
            try
            {
                String content = java.nio.file.Files.readString(nextIdFile, StandardCharsets.UTF_8).trim();
                long nextId = Long.parseLong(content);
                claimStore.setNextClaimId(Math.max(nextId, maxClaimId + 1));
            }
            catch (Exception e)
            {
                // Ignore errors reading next ID
            }
        }

        return migratedCount;
    }

    /**
     * Parse an old-format YAML claim file.
     */
    private CompactClaimStore.ClaimData parseYamlClaimFile(File file, long claimId, List<World> validWorlds) throws Exception
    {
        YamlConfiguration yaml = YamlConfiguration.loadConfiguration(file);

        String lesserCornerStr = yaml.getString("Lesser Boundary Corner");
        String greaterCornerStr = yaml.getString("Greater Boundary Corner");

        if (lesserCornerStr == null || greaterCornerStr == null)
        {
            throw new IllegalArgumentException("Missing boundary corners");
        }

        // Parse coordinates: world;x;y;z
        String[] lesser = lesserCornerStr.split(";");
        String[] greater = greaterCornerStr.split(";");

        if (lesser.length < 4 || greater.length < 4)
        {
            throw new IllegalArgumentException("Invalid coordinate format");
        }

        String worldName = lesser[0];

        // Verify world exists
        World world = null;
        for (World w : validWorlds)
        {
            if (w.getName().equalsIgnoreCase(worldName))
            {
                world = w;
                break;
            }
        }
        if (world == null)
        {
            throw new IllegalArgumentException("World not found: " + worldName);
        }

        CompactClaimStore.ClaimData data = new CompactClaimStore.ClaimData();
        data.id = claimId;
        data.worldName = worldName;
        data.x1 = Integer.parseInt(lesser[1]);
        data.y1 = Integer.parseInt(lesser[2]);
        data.z1 = Integer.parseInt(lesser[3]);
        data.x2 = Integer.parseInt(greater[1]);
        data.y2 = Integer.parseInt(greater[2]);
        data.z2 = Integer.parseInt(greater[3]);

        // Owner
        String ownerStr = yaml.getString("Owner", "");
        if (!ownerStr.isEmpty())
        {
            try
            {
                data.ownerUUID = UUID.fromString(ownerStr);
            }
            catch (IllegalArgumentException e)
            {
                // Admin claim or invalid UUID
                data.ownerUUID = null;
            }
        }

        // Parent claim
        data.parentId = yaml.getLong("Parent Claim ID", -1L);

        // inheritNothing
        data.inheritNothing = yaml.getBoolean("inheritNothing", false);

        // Permissions
        data.builders.addAll(yaml.getStringList("Builders"));
        data.containers.addAll(yaml.getStringList("Containers"));
        data.accessors.addAll(yaml.getStringList("Accessors"));
        data.managers.addAll(yaml.getStringList("Managers"));

        return data;
    }

    /**
     * Create a temporary Claim object for storing in the compact store.
     */
    private me.ryanhamshire.GriefPrevention.Claim createTempClaimForStorage(CompactClaimStore.ClaimData data)
    {
        World world = Bukkit.getWorld(data.worldName);
        if (world == null)
        {
            return null;
        }

        Location lesser = new Location(world, data.x1, data.y1, data.z1);
        Location greater = new Location(world, data.x2, data.y2, data.z2);

        return new me.ryanhamshire.GriefPrevention.Claim(
            lesser,
            greater,
            data.ownerUUID,
            data.builders,
            data.containers,
            data.accessors,
            data.managers,
            data.inheritNothing,
            data.id
        );
    }

    /**
     * Migrate player data from individual files to compact storage.
     */
    private int migratePlayers() throws IOException
    {
        Path playerDataFolder = dataFolder.resolve("PlayerData");
        if (!java.nio.file.Files.exists(playerDataFolder))
        {
            return 0;
        }

        File[] files = playerDataFolder.toFile().listFiles((dir, name) ->
            !name.startsWith("$") && !name.startsWith("_") && !name.endsWith(".ignore") &&
            UUID_PATTERN.matcher(name).matches());

        if (files == null || files.length == 0)
        {
            return 0;
        }

        int migratedCount = 0;

        for (File file : files)
        {
            try
            {
                UUID playerUUID = UUID.fromString(file.getName());
                int[] blocks = parsePlayerDataFile(file);

                if (blocks != null && (blocks[0] != 0 || blocks[1] != 0))
                {
                    playerStore.setPlayerData(playerUUID, blocks[0], blocks[1]);
                    migratedCount++;
                }
            }
            catch (IllegalArgumentException e)
            {
                GriefPrevention.AddLogEntry("Skipping invalid player file: " + file.getName(), CustomLogEntryTypes.Debug);
            }
            catch (Exception e)
            {
                GriefPrevention.AddLogEntry("Error migrating player file " + file.getName() + ": " + e.getMessage(), CustomLogEntryTypes.Exception);
            }
        }

        return migratedCount;
    }

    /**
     * Parse an old-format player data file.
     * Returns [accruedBlocks, bonusBlocks] or null if invalid.
     */
    private int[] parsePlayerDataFile(File file) throws IOException
    {
        List<String> lines = Files.readLines(file, StandardCharsets.UTF_8);

        if (lines.size() < 3)
        {
            return null;
        }

        // Old format:
        // Line 0: blank (was last login timestamp)
        // Line 1: accrued blocks
        // Line 2: bonus blocks
        // Line 3: blank

        try
        {
            int accruedBlocks = Integer.parseInt(lines.get(1).trim());
            int bonusBlocks = Integer.parseInt(lines.get(2).trim());
            return new int[] { accruedBlocks, bonusBlocks };
        }
        catch (NumberFormatException e)
        {
            return null;
        }
    }

    /**
     * Migrate group bonus blocks from individual files.
     */
    private int migrateGroups() throws IOException
    {
        Path playerDataFolder = dataFolder.resolve("PlayerData");
        if (!java.nio.file.Files.exists(playerDataFolder))
        {
            return 0;
        }

        File[] files = playerDataFolder.toFile().listFiles((dir, name) -> name.startsWith("$"));

        if (files == null || files.length == 0)
        {
            return 0;
        }

        int migratedCount = 0;

        for (File file : files)
        {
            try
            {
                String groupName = file.getName().substring(1); // Remove $ prefix
                List<String> lines = Files.readLines(file, StandardCharsets.UTF_8);

                if (!lines.isEmpty())
                {
                    int blocks = Integer.parseInt(lines.get(0).trim());
                    playerStore.setGroupBonusBlocks(groupName, blocks);
                    migratedCount++;
                }
            }
            catch (Exception e)
            {
                GriefPrevention.AddLogEntry("Error migrating group file " + file.getName() + ": " + e.getMessage(), CustomLogEntryTypes.Exception);
            }
        }

        return migratedCount;
    }

    /**
     * Backup old files by renaming the directories.
     */
    private void backupOldFiles() throws IOException
    {
        Path claimDataFolder = dataFolder.resolve("ClaimData");
        Path playerDataFolder = dataFolder.resolve("PlayerData");

        // Find a unique backup suffix
        int backupNum = 1;
        while (java.nio.file.Files.exists(dataFolder.resolve("ClaimData_backup" + backupNum)) ||
               java.nio.file.Files.exists(dataFolder.resolve("PlayerData_backup" + backupNum)))
        {
            backupNum++;
        }

        // Rename ClaimData folder
        if (java.nio.file.Files.exists(claimDataFolder))
        {
            Path backupPath = dataFolder.resolve("ClaimData_backup" + backupNum);
            java.nio.file.Files.move(claimDataFolder, backupPath);
            GriefPrevention.AddLogEntry("Backed up old ClaimData to: " + backupPath);
        }

        // Rename PlayerData folder
        if (java.nio.file.Files.exists(playerDataFolder))
        {
            Path backupPath = dataFolder.resolve("PlayerData_backup" + backupNum);
            java.nio.file.Files.move(playerDataFolder, backupPath);
            GriefPrevention.AddLogEntry("Backed up old PlayerData to: " + backupPath);
        }

        // Create empty directories for the compact storage to use
        java.nio.file.Files.createDirectories(claimDataFolder);
        java.nio.file.Files.createDirectories(playerDataFolder);
    }
}
