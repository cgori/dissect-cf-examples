/*
 *  ========================================================================
 *  DISSECT-CF Examples
 *  ========================================================================
 *  
 *  This file is part of DISSECT-CF Examples.
 *  
 *  DISSECT-CF Examples is free software: you can redistribute it and/or
 *  modify it under the terms of the GNU General Public License as published
 *  by the Free Software Foundation, either version 3 of the License, or (at
 *  your option) any later version.
 *  
 *  DISSECT-CF Examples is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of 
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License along
 *  with DISSECT-CF Examples.  If not, see <http://www.gnu.org/licenses/>.
 *  
 *  (C) Copyright 2019, Gabor Kecskemeti (g.kecskemeti@ljmu.ac.uk)
 */
package uk.ac.ljmu.fet.cs.cloud.examples.autoscaler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import hu.mta.sztaki.lpds.cloud.simulator.iaas.IaaSService;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VirtualMachine;

/**
 * This class applies an autoscaling mechanism, which adjusts the cluster size
 * based on the average utilization of each virtual machine. It also implements
 * an initial pooling mechanism which has 4 (by default) machines boot on
 * creation, allowing for less delay times when requiring to scale.
 * 
 * @author "cgor
 */
public class SolutionAutoScaler extends VirtualInfrastructure {

	public static final double minUtilisationLevelBeforeDestruction = .10; // Minimum threshold for destroying a
																			// machine.
	public static final double maxUtilisationLevelBeforeNewVM = .85; // Maximum threshold for creating a machine.
	public double balance = 0; // Used to balance the cluster.
	public int base_pool = 4; // Having a start of at least 4 virtual machines.
	public static final int hitMax = 30; // Used to remove a virtual machine after hitting this threshold.

	// 2D Array of ranges matching the adjustments array used along side the
	// adjustments array.
	public double[][] ranges = new double[][] { { 0.60, 0.69 }, { 0.70, 0.79 }, { 0.80, 0.89 }, { 0.90, 1 } };
	// Adjustments by 20%, 40%, 60% and 80%.
	public double[] adjustments = new double[] { 1.20, 1.40, 1.60, 1.80 };

	/**
	 * Used to keep track of a virtual machine, and act on it if it hits a certain
	 * threshold of hits.
	 */
	private final HashMap<VirtualMachine, Integer> unnecessaryHits = new HashMap<VirtualMachine, Integer>();

	/**
	 * Initializes the auto scaling mechanism
	 * 
	 * @param cloud the physical infrastructure to use to rent the VMs from
	 */
	public SolutionAutoScaler(final IaaSService cloud) {
		super(cloud);
	}

	/**
	 * Uses the public defined ranges and adjustments to calculate the cluster
	 * adjustment.
	 * 
	 * @param the sum of the average utilization of each virtual machine.
	 * @return adjustment variable
	 */
	public double calculateRange(double averageUtil, int totalVMs) {

		for (int i = 0; i < ranges.length; i++) {

			if (averageUtil >= ranges[i][0] && averageUtil <= ranges[i][1]) {
				return (int) Math.ceil(totalVMs * adjustments[i]) - totalVMs;
				
			}
		}
		return 0;
	}
	/**
	 * The auto scaling mechanism that is run regularly to determine if the virtual
	 * infrastructure needs some changes. The logic is the following:
	 * <ul>
	 * <li>if a VM has less than a given utilisation, then it is destroyed (unless
	 * it is the last VM for a given executable)</li>
	 * <li>if a VM is the last for a given executable, it is given an hour to
	 * receive a new job before it is destructed. <i>After this, one has to
	 * re-register the VM kind to receive new VMs.</i></li>
	 * <li>if an executable was just registered, it will receive a single new
	 * VM.</li>
	 * <li>if all the VMs of a given executable experience an average utilisation of
	 * a given minimum value, then a new VM is created.</li>
	 * 
	 * <li>if we have fewer VMs than the minimum pool base we will create a
	 * VM</li>
	 * <li>the utilization across all machines will be used to balance the cluster
	 * </li>
	 * </ul>
	 * 
	 */
	@Override
	public void tick(long fires) {

		// gets an iterator used to loop through the vmset.
		Iterator<String> kinds = vmSetPerKind.keySet().iterator();
		while (kinds.hasNext()) {
			String kind = kinds.next();
			ArrayList<VirtualMachine> vmset = vmSetPerKind.get(kind);
			// Determining if the base pool is met
			if (vmset.size() < base_pool) {
				// The vmset does not have a base pool of this type yet.
				requestVM(kind);
				continue;
			} else if (vmset.size() == 1) { // check if there is only one machine left
				final VirtualMachine onlyMachine = vmset.get(0);
				// We will try to not destroy the last VM from any kind
				if (onlyMachine.underProcessing.isEmpty() && onlyMachine.toBeAdded.isEmpty()) {
					// It has no ongoing computation
					Integer i = unnecessaryHits.get(onlyMachine);
					if (i == null) {
						unnecessaryHits.put(onlyMachine, 1);
					} else {
						i++;
						if (i < hitMax) { // hitMax is used to set the threshold of hits.
							unnecessaryHits.put(onlyMachine, i);
						} else {
							// After an hour of disuse, we just drop the VM
							unnecessaryHits.remove(onlyMachine);
							destroyVM(onlyMachine);
							kinds.remove();
						}
					}
					// We don't need to check if we need more VMs as it has no computation
					continue;
				}
				// The VM now does some stuff now so we make sure we don't try to remove it
				// prematurely
				unnecessaryHits.remove(onlyMachine);
				// Now we allow the check if we need more VMs.
			} else {
				boolean destroyed = false;
				for (int i = 0; i < vmset.size(); i++) {
					final VirtualMachine vm = vmset.get(i);
					if (vm.underProcessing.isEmpty() && vm.toBeAdded.isEmpty()) {
						// The VM has no task on it at the moment, good candidate
						if (getHourlyUtilisationPercForVM(vm) < minUtilisationLevelBeforeDestruction) {
							// The VM's load was under 20% in the past hour, we might be able to get rid of
							// it
							destroyVM(vm);
						
							destroyed = true;
							i--;
						}
					}
				}
				if (destroyed) {
					// Destroyed VM, reset loop.
					continue;
				}
			}

	
			// Gather the sum of utilization on each machine.
			double subHourUtilSum = 0;
			int totalVMs = vmset.size();
			for (VirtualMachine vm : vmset) {
				subHourUtilSum += getHourlyUtilisationPercForVM(vm);
			}

			double average = subHourUtilSum / totalVMs;
			// Calculate range will return the adjustment for the cluster to be increased
			// by.
			double required = calculateRange(average, totalVMs);
			// Percentage of totalVMs take away to find the difference (the amount to add).
			
			// resize the cluster from required amount.
			for (int i = 0; i < required; i++) {
				requestVM(kind);
			}
		}
	}
}
