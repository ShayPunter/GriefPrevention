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

import me.ryanhamshire.GriefPrevention.*;
import org.bukkit.Bukkit;
import org.bukkit.Location;
import org.bukkit.World;
import org.bukkit.scheduler.BukkitTask;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A data store implementation that uses the compact storage format.
 * This consolidates all claims into per-world files and all player data
 * into a single file, dramatically reducing filesystem overhead.
 */
public class CompactFlatFileDataStore extends DataStore
{
    private final Path dataFolder;
    private final CompactClaimStore claimStore;
    private final CompactPlayerStore playerStore;
    private final boolean useCompression;

    // Periodic save task
    private BukkitTask saveTask;
    private static final long SAVE_INTERVAL_TICKS = 20L * 60 * 5; // 5 minutes

    public CompactFlatFileDataStore(boolean useCompression) throws Exception
    {
        this.dataFolder = Path.of(dataLayerFolderPath);
        this.useCompression = useCompression;

        // Initialize storage components
        Path claimDataPath = dataFolder.resolve("ClaimData");
        Path playerDataPath = dataFolder.resolve("PlayerData");

        this.claimStore = new CompactClaimStore(claimDataPath, useCompression);
        this.playerStore = new CompactPlayerStore(playerDataPath, useCompression);

        initialize();
    }

    /**
     * Check if there's existing data that can be loaded.
     */
    public static boolean hasData()
    {
        Path claimDataFolder = Path.of(dataLayerFolderPath, "ClaimData");
        return Files.exists(claimDataFolder);
    }

    /**
     * Check if migration from old format is needed.
     */
    public static boolean needsMigration()
    {
        Path dataFolder = Path.of(dataLayerFolderPath);
        StorageMigrator migrator = new StorageMigrator(dataFolder, null, null);
        return migrator.needsMigration();
    }

    @Override
    void initialize() throws Exception
    {
        // Ensure data folders exist
        Files.createDirectories(dataFolder);
        Files.createDirectories(dataFolder.resolve("ClaimData"));
        Files.createDirectories(dataFolder.resolve("PlayerData"));

        // Check if we need to migrate from old format
        StorageMigrator migrator = new StorageMigrator(dataFolder, claimStore, playerStore);
        if (migrator.needsMigration())
        {
            GriefPrevention.AddLogEntry("Detected old storage format. Starting migration...");
            if (!migrator.migrate())
            {
                throw new Exception("Failed to migrate from old storage format. Check logs for details.");
            }
        }

        // Initialize storage systems
        claimStore.initialize();
        playerStore.initialize();

        // Load all claims into memory
        loadAllClaims();

        // Load group data
        loadGroupData();

        // Start periodic save task
        startSaveTask();

        // Call parent initialization
        super.initialize();
    }

    /**
     * Load all claims from the compact store into memory.
     */
    private void loadAllClaims()
    {
        Map<Long, Claim> loadedClaims = new ConcurrentHashMap<>();
        List<CompactClaimStore.ClaimData> orphanSubclaims = new ArrayList<>();

        // First pass: load all claims
        for (CompactClaimStore.ClaimData claimData : claimStore.getAllClaims())
        {
            Claim claim = claimStore.dataToClaim(claimData, null);
            if (claim != null)
            {
                loadedClaims.put(claimData.id, claim);

                if (claimData.parentId >= 0)
                {
                    orphanSubclaims.add(claimData);
                }
            }
        }

        // Second pass: link subclaims to parents
        for (CompactClaimStore.ClaimData claimData : orphanSubclaims)
        {
            Claim subclaim = loadedClaims.get(claimData.id);
            Claim parent = loadedClaims.get(claimData.parentId);

            if (subclaim != null && parent != null)
            {
                subclaim.parent = parent;
                parent.children.add(subclaim);
            }
        }

        // Add all top-level claims to the data store
        for (Claim claim : loadedClaims.values())
        {
            if (claim.parent == null)
            {
                this.addClaim(claim, false);
            }
        }

        // Update next claim ID
        this.nextClaimID = claimStore.getNextClaimId();
    }

    /**
     * Load group bonus block data.
     */
    private void loadGroupData()
    {
        for (Map.Entry<String, Integer> entry : playerStore.getAllGroupBonusBlocks().entrySet())
        {
            this.permissionToBonusBlocksMap.put(entry.getKey(), entry.getValue());
        }
    }

    /**
     * Start the periodic save task.
     */
    private void startSaveTask()
    {
        if (GriefPrevention.instance != null && GriefPrevention.instance.isEnabled())
        {
            saveTask = Bukkit.getScheduler().runTaskTimerAsynchronously(
                GriefPrevention.instance,
                this::saveAllDirty,
                SAVE_INTERVAL_TICKS,
                SAVE_INTERVAL_TICKS
            );
        }
    }

    /**
     * Save all dirty data.
     */
    private void saveAllDirty()
    {
        try
        {
            claimStore.saveAllDirty();
            playerStore.saveAllDirty();
        }
        catch (Exception e)
        {
            GriefPrevention.AddLogEntry("Error in periodic save: " + e.getMessage(), CustomLogEntryTypes.Exception);
        }
    }

    @Override
    synchronized void writeClaimToStorage(Claim claim)
    {
        claimStore.markDirty(claim);
    }

    @Override
    synchronized void deleteClaimFromSecondaryStorage(Claim claim)
    {
        String worldName = claim.getLesserBoundaryCorner().getWorld().getName();
        claimStore.markDeleted(claim.getID(), worldName);
    }

    @Override
    synchronized PlayerData getPlayerDataFromStorage(UUID playerID)
    {
        PlayerData playerData = new PlayerData();
        playerData.playerID = playerID;

        CompactPlayerStore.PlayerBlockData blockData = playerStore.getPlayerData(playerID);
        if (blockData != null)
        {
            playerData.setAccruedClaimBlocks(blockData.accruedBlocks);
            playerData.setBonusClaimBlocks(blockData.bonusBlocks);
        }
        else
        {
            // Default values will be set by PlayerData class
        }

        return playerData;
    }

    @Override
    public void overrideSavePlayerData(UUID playerID, PlayerData playerData)
    {
        if (playerID == null) return;

        int accrued = playerData.getAccruedClaimBlocks();
        int bonus = playerData.getBonusClaimBlocks();

        playerStore.setPlayerData(playerID, accrued, bonus);
    }

    @Override
    synchronized void incrementNextClaimID()
    {
        this.nextClaimID = claimStore.getNextClaimId();
    }

    @Override
    synchronized void saveGroupBonusBlocks(String groupName, int currentValue)
    {
        playerStore.setGroupBonusBlocks(groupName, currentValue);
    }

    @Override
    int getSchemaVersionFromStorage()
    {
        // The compact format always uses the latest schema
        return DataStore.latestSchemaVersion;
    }

    @Override
    void updateSchemaVersionInStorage(int versionToSet)
    {
        // Schema version is implicit in the compact format
    }

    @Override
    synchronized void close()
    {
        // Cancel save task
        if (saveTask != null)
        {
            saveTask.cancel();
        }

        // Force save all pending data
        try
        {
            GriefPrevention.AddLogEntry("Saving all data before shutdown...");
            claimStore.saveAll();
            playerStore.saveAll();
            GriefPrevention.AddLogEntry("Data saved successfully.");
        }
        catch (Exception e)
        {
            GriefPrevention.AddLogEntry("Error saving data on shutdown: " + e.getMessage(), CustomLogEntryTypes.Exception);
            e.printStackTrace();
        }
    }

    /**
     * Get statistics about the storage.
     */
    public StorageStats getStats()
    {
        StorageStats stats = new StorageStats();
        stats.totalClaims = this.claims.size();
        stats.totalSubclaims = 0;
        for (Claim claim : this.claims)
        {
            stats.totalSubclaims += claim.children.size();
        }
        stats.totalPlayers = playerStore.getPlayerCount();
        stats.totalGroups = this.permissionToBonusBlocksMap.size();
        stats.useCompression = this.useCompression;
        return stats;
    }

    /**
     * Statistics about the storage.
     */
    public static class StorageStats
    {
        public int totalClaims;
        public int totalSubclaims;
        public int totalPlayers;
        public int totalGroups;
        public boolean useCompression;

        @Override
        public String toString()
        {
            return String.format("Claims: %d (+%d subclaims), Players: %d, Groups: %d, Compression: %s",
                totalClaims, totalSubclaims, totalPlayers, totalGroups, useCompression ? "enabled" : "disabled");
        }
    }
}
